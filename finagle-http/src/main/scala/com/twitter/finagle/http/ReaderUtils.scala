package com.twitter.finagle.http

import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, Promise, Throw}
import org.jboss.netty.handler.codec.http.{HttpChunk, DefaultHttpChunk}
import org.jboss.netty.buffer.ChannelBuffers

private[http] object NullReader extends Reader {
  def read(n: Int) = ReaderUtils.eof
  def discard() { }
}

private[http] object ReaderUtils {
  val eof = Future.value(Buf.Eof)

  /**
   * Implement a Reader given a Transport. The Reader represents a byte
   * stream, so it is useful to know when the stream has finished. This end of
   * stream signal provided to the caller by way of `done`, which is resolved
   * when the stream is done.
   */
  def readerFromTransport(trans: Transport[Any, Any], done: Promise[Unit]): Reader =
    new Reader {
      private[this] val mu = new AsyncMutex
      @volatile private[this] var buf = Buf.Empty

      // We don't want to rely on scheduler semantics for ordering.
      def read(n: Int): Future[Buf] = mu.acquire() flatMap { permit =>
        val readOp = if (buf eq Buf.Eof) {
          eof
        } else if (buf.length > 0) {
          val f = Future.value(buf.slice(0, n))
          buf = buf.slice(n, Int.MaxValue)
          f
        } else trans.read() flatMap {
          // Buf is empty so set it to the result of trans.read()
          case chunk: HttpChunk if chunk.isLast =>
            done.setDone()
            buf = Buf.Eof
            eof

          case chunk: HttpChunk =>
            // Read data -- return up to n bytes and save the rest
            val cbb = ChannelBufferBuf(chunk.getContent)
            val f = Future.value(cbb.slice(0, n))
            buf = cbb.slice(n, Int.MaxValue)
            f

          case invalid =>
            val exc = new IllegalArgumentException(
              "invalid message \"%s\"".format(invalid))
            Future.exception(exc)
        }

        readOp onFailure { exc =>
          trans.close()
          done.updateIfEmpty(Throw(exc))
        } ensure { permit.release() }
      }

      def discard() {
        // Any interrupt to `read` will result in transport closure, but we also
        // call `trans.close` here to handle the case where a discard is called
        // without interrupting the `read` operation.
        trans.close()
      }
    }

  /**
   * Translates a Buf into HttpChunk. Beware: Buf.Empty will have the same
   * effect as Buf.Eof, which if used incorrectly can prematurely signal the end
   * of stream.
   */
  def chunkOfBuf(buf: Buf): HttpChunk = buf match {
    case Buf.Eof =>
      HttpChunk.LAST_CHUNK
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
      // Ignore Buf.Empty as they can prematurely end the stream.
      case buf if buf eq Buf.Empty => streamChunks(trans, r)
      case buf =>
        val chunk = chunkOfBuf(buf)
        if (chunk.isLast) trans.write(chunk)
        else trans.write(chunk) before streamChunks(trans, r)
    }
}
