package com.twitter.finagle.http2.transport

import com.twitter.finagle.FailureFlags
import com.twitter.logging.{HasLogLevel, Level, Logger}
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http2.{Http2ResetFrame, Http2WindowUpdateFrame}
import io.netty.util.ReferenceCountUtil
import scala.util.control.NoStackTrace

/**
 * A handler to rectify any other Http2StreamMessage objects.
 *
 * The Netty `Http2MultiplexCodec` passes `Http2StreamFrame` messages to the child pipeline
 * including
 * - Http2HeadersFrame
 * - Http2DataFrame
 * - Http2ResetFrame
 * - Http2WindowUpdateFrame
 *
 * The first two are converted to `HttpObject` representations by the
 * `Http2StreamFrameToHttpObjectCodec` but the latter two still propagate forward and
 * are handled here.
 *
 * @note this __must__ come after a `Http2StreamFrameToHttpObjectCodec` in the child pipeline
 *       since it expects HEADERS and DATA frames to have been converted to their `HttpObject`
 *       representations.
 */
private[http2] final class Http2StreamMessageHandler extends ChannelDuplexHandler {
  // After we've received a reset frame from the peer, there is no longer
  // any reason to send stream messages to the other session.
  private[this] var resetErrorCode: Option[Long] = None

  override def write(ctx: ChannelHandlerContext, msg: Object, p: ChannelPromise): Unit = resetErrorCode match {
    case None =>
      super.write(ctx, msg, p)
    case Some(code) =>
      ReferenceCountUtil.release(msg)
      p.tryFailure(new ClientDiscardedRequestException(code))
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case update: Http2WindowUpdateFrame =>
      // We don't care about these at this level so just release it.
      ReferenceCountUtil.release(update)


    case rst: Http2ResetFrame =>
      // we don't propagate reset frames because http/1.1 doesn't know how to handle it.
      resetErrorCode = Some(rst.errorCode)
      ReferenceCountUtil.release(msg)

    case httpObject: HttpObject =>
      super.channelRead(ctx, httpObject)

    case other =>
      // At this point we should have cleaned up all the non-HTTP/1.1 messages from the
      // pipeline and anything else will gum up the works. So, if we find anything else,
      // we need to log really loudly and make sure to release it (if it's ref-counted).
      ReferenceCountUtil.release(other)
      val msg = s"Unexpected pipeline message: ${other.getClass.getName}"
      val ex = new IllegalArgumentException(msg)
      Http2StreamMessageHandler.logger.error(ex, msg)
      ctx.fireExceptionCaught(ex)
  }
}

private object Http2StreamMessageHandler {
  private val logger = Logger.get(classOf[Http2StreamMessageHandler])
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
