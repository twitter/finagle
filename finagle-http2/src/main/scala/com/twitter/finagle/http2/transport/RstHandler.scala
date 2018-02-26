package com.twitter.finagle.http2.transport

import com.twitter.finagle.FailureFlags
import com.twitter.logging.{HasLogLevel, Level}
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http2.Http2ResetFrame
import io.netty.util.ReferenceCountUtil
import scala.util.control.NoStackTrace

/**
 * A handler to drop incoming reset frames, and ensure servers don't try to
 * write messages to closed streams.
 */
private[http2] class RstHandler extends ChannelDuplexHandler {
  private[this] var errorCode: Option[Long] = None

  override def write(ctx: ChannelHandlerContext, msg: Object, p: ChannelPromise): Unit = errorCode match {
    case None =>
      super.write(ctx, msg, p)
    case Some(code) =>
      ReferenceCountUtil.release(msg)
      p.tryFailure(new ClientDiscardedRequestException(code))
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case rst: Http2ResetFrame =>
      // we don't propagate reset frames because http/1.1 doesn't know how to handle it.
      errorCode = Some(rst.errorCode)
      ReferenceCountUtil.release(msg)
    case _ =>
      super.channelRead(ctx, msg)
  }
}

class ClientDiscardedRequestException private[transport] (
  errorCode: Long,
  private[finagle] val flags: Long = FailureFlags.NonRetryable
) extends Exception(
      s"Attempted to write to a stream after receiving an RST with error code $errorCode"
    )
    with FailureFlags[ClientDiscardedRequestException]
    with HasLogLevel
    with NoStackTrace {
  def logLevel: Level = Level.DEBUG
  protected def copyWithFlags(flags: Long): ClientDiscardedRequestException =
    new ClientDiscardedRequestException(errorCode, flags)
}
