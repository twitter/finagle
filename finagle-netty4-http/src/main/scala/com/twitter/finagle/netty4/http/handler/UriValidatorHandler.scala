package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.netty4.http.util.UriUtils
import com.twitter.finagle.netty4.http.util.UriUtils.InvalidUriException
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.DecoderResult
import io.netty.handler.codec.http.{HttpObject, HttpRequest}

/**
 * All inbound URIs are validated in the Netty pipeline so we can
 * reject malformed requests earlier, before they enter the Finagle land. This pipeline handler
 * does exactly this. Similar to other Netty components, it sets HTTP object's `DecoderResult` to a
 * failure so the next handler(s) can take an appropriate action (reject if it's a server; convert
 * to an exception if it's a client).
 */
@Sharable
private[finagle] object UriValidatorHandler extends ChannelInboundHandlerAdapter {

  val HandlerName: String = "uriValidationHandler"

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    msg match {
      case req: HttpRequest =>
        validateUri(ctx, req, req.uri())
      case _ => ()
    }

    ctx.fireChannelRead(msg)
  }

  private[this] def validateUri(ctx: ChannelHandlerContext, obj: HttpObject, uri: String): Unit =
    if (!UriUtils.isValidUri(uri)) {
      obj.setDecoderResult(DecoderResult.failure(new InvalidUriException(uri)))
    }

}
