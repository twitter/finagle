package com.twitter.finagle.http2

import com.twitter.finagle.StreamClosedException
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

final class GoAwayException(val errorCode: Long, streamId: Long, remoteAddress: Option[SocketAddress])
  extends StreamClosedException(remoteAddress, streamId.toString, s"GOAWAY(${Http2StreamClosedException.errorToString(errorCode)})")

final class RstException(val errorCode: Long, streamId: Long, remoteAddress: Option[SocketAddress])
  extends StreamClosedException(remoteAddress, streamId.toString, s"RST(${Http2StreamClosedException.errorToString(errorCode)})")
