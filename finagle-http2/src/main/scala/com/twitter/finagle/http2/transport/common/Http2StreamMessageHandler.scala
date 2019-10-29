package com.twitter.finagle.http2.transport.common

import com.twitter.finagle.FailureFlags
import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.http2.RstException
import com.twitter.logging.{HasLogLevel, Level, Logger}
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.http.{
  DefaultFullHttpResponse,
  FullHttpResponse,
  HttpObject,
  HttpResponseStatus,
  HttpVersion
}
import io.netty.handler.codec.http2.{Http2Error, Http2ResetFrame, Http2WindowUpdateFrame}
import io.netty.util.ReferenceCountUtil
import scala.util.control.NoStackTrace

/**
 * A handler to rectify any other Http2StreamMessage objects.
 *
 * The Netty `Http2MultiplexHandler` passes `Http2StreamFrame` messages to the child pipeline
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

  override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit = evt match {
    case rst: Http2ResetFrame =>
      resetErrorCode = Some(rst.errorCode)
      handleReset(ctx, rst, observedFirstHttpObject)

    case _ =>
      ctx.fireUserEventTriggered(evt)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case update: Http2WindowUpdateFrame =>
      // We don't care about these at this level so just release it.
      ReferenceCountUtil.release(update)

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
        convertAndFireRst(ctx, rst)
      }
    }
  }

  private[this] def convertAndFireRst(ctx: ChannelHandlerContext, rst: Http2ResetFrame): Unit = {
    val rstCode = rst.errorCode

    // For nack RST frames we want to reuse the machinery in ClientNackFilter
    // so we synthesize an appropriate 503 response and fire it down the pipeline.
    // For all other RST frames something bad happened so we push a RstException
    // down the exception pathway that will close the pipeline.
    if (rstCode == Http2Error.REFUSED_STREAM.code) {
      ctx.fireChannelRead(syntheticNackResponse(HttpNackFilter.RetryableNackHeader))
    } else if (rstCode == Http2Error.ENHANCE_YOUR_CALM.code) {
      ctx.fireChannelRead(syntheticNackResponse(HttpNackFilter.NonRetryableNackHeader))
    } else {
      // If we don't have a handle on the stream use -1 as a sentinel. In
      // practice this should be exceedingly rare or never happen since we
      // are part of the stream pipeline.
      val streamId = if (rst.stream != null) rst.stream.id else -1
      ctx.fireExceptionCaught(
        new RstException(rstCode, streamId, Option(ctx.channel.remoteAddress), FailureFlags.Empty))
    }
  }

  private[this] def syntheticNackResponse(nackHeader: String): FullHttpResponse = {
    val resp =
      new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.SERVICE_UNAVAILABLE,
        Unpooled.buffer(0))
    resp.headers.set(nackHeader, "true")
    resp
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
