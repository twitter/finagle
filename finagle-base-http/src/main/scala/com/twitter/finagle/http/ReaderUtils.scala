package com.twitter.finagle.http

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, Return}
import org.jboss.netty.handler.codec.http.{HttpChunk, DefaultHttpChunk}

private[http] object ReaderUtils {
  /**
   * Serialize an HttpChunk into a Buf.
   */
  def readChunk(chunk: Any): Future[Option[Buf]] = chunk match {
    case chunk: HttpChunk if chunk.isLast =>
      Future.None

    case chunk: HttpChunk =>
      Future.value(Some(ChannelBufferBuf.Owned(chunk.getContent.duplicate)))

    case invalid =>
      val exc = new IllegalArgumentException(
        "invalid message \"%s\"".format(invalid))
      Future.exception(exc)
  }

  /**
   * Translates a Buf into HttpChunk. Beware: an empty buffer indicates end
   * of stream.
   */
  def chunkOfBuf(buf: Buf): HttpChunk =
    new DefaultHttpChunk(ChannelBufferBuf.Owned.extract(buf))

  /**
   * Continuously read from a Reader, writing everything to a Transport.
   */
  def streamChunks(
    trans: Transport[Any, Any],
    r: Reader,
    // TODO Find a better number for bufSize, e.g. 32KiB - Buf overhead
    bufSize: Int = Int.MaxValue
  ): Future[Unit] = {
    r.read(bufSize) flatMap {
      case None =>
        trans.write(HttpChunk.LAST_CHUNK)
      case Some(buf) =>
        trans.write(chunkOfBuf(buf)) transform {
          case Return(_) => streamChunks(trans, r, bufSize)
          case _ => Future(r.discard())
        }
    }
  }
}
