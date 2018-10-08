package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.http.{Fields, TooLongMessageException}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http2.Http2Exception.HeaderListSizeException

/**
 * A Netty pipeline stage that converts events to fit the finagle client model
 *
 * This stage translates certain exceptions and modifies the inbound response to
 * trigger closing of the `Transport` since each request will get it's own Netty
 * stream channel from the `Http2MultiplexCodec`.
 */
@Sharable
object Http2ClientEventMapper extends ChannelInboundHandlerAdapter {
  override def exceptionCaught(
    ctx: ChannelHandlerContext,
    cause: Throwable
  ): Unit = {
    val ex = cause match {
      case ex: HeaderListSizeException => TooLongMessageException(ex, ctx.channel.remoteAddress)
      case other => other
    }

    super.exceptionCaught(ctx, ex)
  }

  override def channelRead(
    ctx: ChannelHandlerContext,
    msg: scala.Any
  ): Unit = {
    msg match {
        // We don't reuse streams, so we need to alert the dispatcher that we're closed.
      case resp: HttpResponse =>
        resp.headers.set(Fields.Connection, "close")

      case _ => () // noop
    }
    ctx.fireChannelRead(msg)
  }
}
