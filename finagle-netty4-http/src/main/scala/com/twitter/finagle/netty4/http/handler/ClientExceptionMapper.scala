package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.TooLongFrameException

/** Map some Netty 4 http client related exceptions to Finagle specific exceptions */
@Sharable
private[http] object ClientExceptionMapper extends ChannelInboundHandlerAdapter {
  override def exceptionCaught(
    ctx: ChannelHandlerContext,
    cause: Throwable
  ): Unit = {
    val next = cause match {
      case e: TooLongFrameException =>
        http.TooLongMessageException(e, ctx.channel().remoteAddress())
      case other => other
    }

    ctx.fireExceptionCaught(next)
  }
}
