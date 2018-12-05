package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.RstException
import com.twitter.finagle.FailureFlags
import com.twitter.logging.{HasLogLevel, Level, Logger}
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http2.{Http2Error, Http2ResetFrame, Http2WindowUpdateFrame}
import io.netty.util.ReferenceCountUtil
import java.net.SocketAddress
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
private[http2] abstract class Http2StreamMessageHandler private () extends ChannelDuplexHandler {
  // After we've received a reset frame from the peer, there is no longer
  // any reason to send stream messages to the other session.
  private[this] var resetErrorCode: Option[Long] = None
  private[this] var observedFirstHttpObject = false

  protected def handleReset(
    ctx: ChannelHandlerContext,
    rst: Http2ResetFrame,
    observedFirstHttpObject: Boolean
  ): Unit

  override def write(ctx: ChannelHandlerContext, msg: Object, p: ChannelPromise): Unit =
    resetErrorCode match {
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
      resetErrorCode = Some(rst.errorCode)
      handleReset(ctx, rst, observedFirstHttpObject)

    case httpObject: HttpObject =>
      observedFirstHttpObject = true
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

private[http2] object Http2StreamMessageHandler {
  private val logger = Logger.get(classOf[Http2StreamMessageHandler])

  def apply(isServer: Boolean): Http2StreamMessageHandler =
    if (isServer) new ServerHttp2StreamMessageHandler
    else new ClientHttp2StreamMessageHandler

  private final class ServerHttp2StreamMessageHandler extends Http2StreamMessageHandler {
    protected def handleReset(
      ctx: ChannelHandlerContext,
      rst: Http2ResetFrame,
      observedFirstHttpObject: Boolean
    ): Unit = {
      // Server can't do anything about it, so we just swallow it.
      ()
    }
  }

  private final class ClientHttp2StreamMessageHandler extends Http2StreamMessageHandler {
    protected def handleReset(
      ctx: ChannelHandlerContext,
      rst: Http2ResetFrame,
      observedFirstHttpObject: Boolean
    ): Unit = {
      // If this is the first message, we fire our own exception down the pipeline
      // which may be a nack.
      if (!observedFirstHttpObject) {
        ctx.fireExceptionCaught(rstToException(rst, Option(ctx.channel.remoteAddress)))
      }
    }
  }

  private[this] def rstToException(
    rst: Http2ResetFrame,
    remoteAddress: Option[SocketAddress]
  ): RstException = {
    val rstCode = rst.errorCode
    val flags = if (rstCode == Http2Error.REFUSED_STREAM.code) {
      FailureFlags.Retryable | FailureFlags.Rejected
    } else if (rstCode == Http2Error.ENHANCE_YOUR_CALM.code) {
      FailureFlags.Rejected | FailureFlags.NonRetryable
    } else {
      FailureFlags.Empty
    }

    new RstException(rstCode, rst.stream.id, remoteAddress, flags)
  }
}

class ClientDiscardedRequestException private[transport] (
  errorCode: Long,
  val flags: Long = FailureFlags.NonRetryable)
    extends Exception(
      s"Attempted to write to a stream after receiving an RST with error code $errorCode"
    )
    with FailureFlags[ClientDiscardedRequestException]
    with HasLogLevel
    with NoStackTrace {
  def logLevel: Level = Level.DEBUG
  protected def copyWithFlags(flags: Long): ClientDiscardedRequestException =
    new ClientDiscardedRequestException(errorCode, flags)
}
