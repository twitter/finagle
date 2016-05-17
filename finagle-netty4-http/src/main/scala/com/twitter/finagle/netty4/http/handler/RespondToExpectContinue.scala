package com.twitter.finagle.netty4.http.handler

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http._


/**
 * Categorically respond 100 CONTINUE to message bearing the 'expect: continue 100' header
 *
 * see longer note in [[exp.Netty4HttpListener]]
 * why we need this.
 */
@Sharable
private[http] object RespondToExpectContinue extends ChannelInboundHandlerAdapter {

  def newContinue(): FullHttpResponse =
    new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.CONTINUE,
      Unpooled.EMPTY_BUFFER)

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case http: HttpMessage if HttpUtil.is100ContinueExpected(http) =>
        ctx.writeAndFlush(newContinue())
        http.headers.remove(HttpHeaderNames.EXPECT)
      case _ =>
    }
    super.channelRead(ctx, msg)
  }
}
