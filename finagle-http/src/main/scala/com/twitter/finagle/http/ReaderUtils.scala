package com.twitter.finagle.http

import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, Promise, Throw}
import org.jboss.netty.handler.codec.http.{HttpChunk, DefaultHttpChunk}
import org.jboss.netty.buffer.ChannelBuffers

private[http] object NullReader extends Reader {
  def read(n: Int) = Future.None
  def discard() { }
}

private[http] object ReaderUtils {
  /**
   * Implement a Reader given a Transport. The Reader represents a byte
   * stream, so it is useful to know when the stream has finished. This end of
   * stream signal provided to the caller by way of `done`, which is resolved
   * when the stream is done.
   */
  def readerFromTransport(trans: Transport[Any, Any], done: Promise[Unit]): Reader =
    new Reader {
      private[this] val mu = new AsyncMutex
      @volatile private[this] var buf: Option[Buf] = Some(Buf.Empty)  // None = EOF

      private[this] def fill(): Future[Unit] = {
        trans.read() flatMap {
          // Buf is empty so set it to the result of trans.read()
          case chunk: HttpChunk if chunk.isLast =>
            buf = None
            Future.Done

          case chunk: HttpChunk =>
            // Read data -- return up to n bytes and save the rest
            buf = Some(ChannelBufferBuf(chunk.getContent))
            Future.Done

          case invalid =>
            val exc = new IllegalArgumentException(
              "invalid message \"%s\"".format(invalid))
            buf = None
            Future.exception(exc)
        }
      }

      def read(n: Int): Future[Option[Buf]] = 
        mu.acquire() flatMap { permit =>
          def go(): Future[Option[Buf]] = buf match {
            case None =>
              done.setDone()
              Future.None
            case Some(buf) if buf.isEmpty => 
              fill() before go()
            case Some(nonempty) =>
              val f = Future.value(Some(nonempty.slice(0, n)))
              buf = Some(nonempty.slice(n, Int.MaxValue))
              f
          }

          go() onFailure { exc =>
            trans.close()
            done.updateIfEmpty(Throw(exc))
          } ensure { permit.release() }
        }

      def discard() {
        // Any interrupt to `read` will result in transport closure, but we also
        // call `trans.close()` here to handle the case where a discard is called
        // without interrupting the `read` operation.
        trans.close()
      }
    }

  /**
   * Translates a Buf into HttpChunk. Beware: an empty buffer indicates end
   * of stream.
   */
  def chunkOfBuf(buf: Buf): HttpChunk = buf match {
    case cb: ChannelBufferBuf =>
      new DefaultHttpChunk(cb.buf)
    case buf =>
      new DefaultHttpChunk(BufChannelBuffer(buf))
  }

  /**
   * Continuously read from a Reader, writing everything to a Transport.
   */
  def streamChunks(
    trans: Transport[Any, Any],
    r: Reader,
    // TODO Find a better number for bufSize, e.g. 32KiB - Buf overhead
    bufSize: Int = Int.MaxValue
  ): Future[Unit] =
    r.read(bufSize) flatMap {
      case None =>
        trans.write(HttpChunk.LAST_CHUNK)
      case Some(buf) =>
        trans.write(chunkOfBuf(buf)) before streamChunks(trans, r)
    }
}
