package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.InvalidUriException
import com.twitter.finagle.netty4.http.util.UriUtils
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.handler.codec.DecoderResult
import io.netty.handler.codec.http.{HttpObject, HttpRequest}
import io.netty.util.ReferenceCountUtil

/**
 * All inbound URIs are validated in the Netty pipeline so we can
 * reject malformed requests earlier, before they enter the Finagle land. This pipeline handler
 * does exactly this. Similar to other Netty components, it sets HTTP object's `DecoderResult` to a
 * failure so the next handler(s) can take an appropriate action (reject if it's a server; convert
 * to an exception if it's a client).
 */
@Sharable
private[finagle] object UriValidatorHandler extends ChannelDuplexHandler {

  val HandlerName: String = "uriValidatorHandler"

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    msg match {
      case req: HttpRequest =>
        validateUri(req, req.uri())
      case _ => ()
    }

    ctx.fireChannelRead(msg)
  }

  override def write(
    ctx: ChannelHandlerContext,
    msg: Any,
    promise: ChannelPromise
  ): Unit =
    msg match {
      case req: HttpRequest if !UriUtils.isValidUri(req.uri()) =>
        ReferenceCountUtil.release(msg)
        ctx.fireExceptionCaught(InvalidUriException(req.uri()))
      case _ =>
        super.write(ctx, msg, promise)
    }

  private[this] def validateUri(obj: HttpObject, uri: String): Unit =
    if (!UriUtils.isValidUri(uri)) {
      obj.setDecoderResult(DecoderResult.failure(InvalidUriException(uri)))
    }

}
