package com.twitter.finagle.http

import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, Promise, Return}
import org.jboss.netty.handler.codec.http.{HttpChunk, DefaultHttpChunk}
import org.jboss.netty.buffer.ChannelBuffers

private[http] object ReaderUtils {
  /**
   * Serialize an HttpChunk into a Buf.
   */
  def readChunk(chunk: Any): Future[Option[Buf]] = chunk match {
    case chunk: HttpChunk if chunk.isLast =>
      Future.None

    case chunk: HttpChunk =>
      Future.value(Some(ChannelBufferBuf(chunk.getContent)))

    case invalid =>
      val exc = new IllegalArgumentException(
        "invalid message \"%s\"".format(invalid))
      Future.exception(exc)
  }

  /**
   * Translates a Buf into HttpChunk. Beware: an empty buffer indicates end
   * of stream.
   */
  def chunkOfBuf(buf: Buf): HttpChunk = buf match {
    case ChannelBufferBuf.Unsafe(buf) =>
      new DefaultHttpChunk(buf)
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
        trans.write(chunkOfBuf(buf)) before 
          streamChunks(trans, r)
    }
}
