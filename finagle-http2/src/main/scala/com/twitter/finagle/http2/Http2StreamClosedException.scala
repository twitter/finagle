package com.twitter.finagle.http2

import com.twitter.finagle.{FailureFlags, StreamClosedException}
import io.netty.handler.codec.http2.Http2Error
import java.net.SocketAddress

private object Http2StreamClosedException {
  private[http2] def errorToString(errorCode: Long): String = {
    val err = Http2Error.valueOf(errorCode)
    if (err == null) // happens for unknown codes
      "unknown error code"
    else
      err.toString
  }
}

final class GoAwayException private[http2] (
  val errorCode: Long,
  streamId: Long,
  remoteAddress: Option[SocketAddress],
  flags: Long)
    extends StreamClosedException(
      remoteAddress,
      streamId,
      flags
    ) {
  def this(errorCode: Long, streamId: Long, remoteAddress: Option[SocketAddress]) =
    this(errorCode: Long, streamId: Long, remoteAddress: Option[SocketAddress], FailureFlags.Empty)

  protected def whyFailed: String =
    s"GOAWAY(${Http2StreamClosedException.errorToString(errorCode)}, $streamId)"

  protected def copyWithFlags(flags: Long): GoAwayException =
    new GoAwayException(errorCode, streamId, remoteAddress, flags)
}

final class RstException private[http2] (
  val errorCode: Long,
  streamId: Long,
  remoteAddress: Option[SocketAddress],
  flags: Long)
    extends StreamClosedException(
      remoteAddress,
      streamId,
      flags
    ) {

  def this(errorCode: Long, streamId: Long, remoteAddress: Option[SocketAddress]) =
    this(errorCode: Long, streamId: Long, remoteAddress: Option[SocketAddress], FailureFlags.Empty)

  protected def whyFailed: String =
    s"RST(${Http2StreamClosedException.errorToString(errorCode)})"

  protected def copyWithFlags(flags: Long): RstException =
    new RstException(errorCode, streamId, remoteAddress, flags)
}
