package com.twitter.finagle.http4

import com.twitter.finagle.netty4.{BufAsByteBuf, ByteBufAsBuf}
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, Return}
import io.netty.handler.codec.{http => NettyHttp}

private[http4] object ReaderUtils {
  /**
   * Serialize a http chunk into a Buf.
   */
  def readChunk(chunk: Any): Future[Option[Buf]] = chunk match {
    case chunk: NettyHttp.LastHttpContent =>
      Future.None

    case chunk: NettyHttp.HttpContent =>
      Future.value(Some(ByteBufAsBuf.Owned(chunk.content.duplicate)))

    case invalid =>
      Future.exception(
        new IllegalArgumentException("invalid message \"%s\"".format(invalid))
      )
  }

  /**
   * Translates a Buf into HttpContent. Beware: an empty buffer indicates end
   * of stream.
   */
  def chunkOfBuf(buf: Buf): NettyHttp.HttpContent =
    new NettyHttp.DefaultHttpContent(BufAsByteBuf.Owned(buf))

  /**
   * Continuously read from a Reader, writing everything to a Transport.
   */
  def streamChunks(
    trans: Transport[Any, Any],
    r: Reader,
    // TODO Find a better number for bufSize, e.g. 32KiB - Buf overhead
    bufSize: Int = Int.MaxValue
  ): Future[Unit] = {
    r.read(bufSize).flatMap {
      case None =>
        trans.write(NettyHttp.LastHttpContent.EMPTY_LAST_CONTENT)
      case Some(buf) =>
        trans.write(chunkOfBuf(buf)).transform {
          case Return(_) => streamChunks(trans, r, bufSize)
          case _ => Future(r.discard())
        }
    }
  }
}
