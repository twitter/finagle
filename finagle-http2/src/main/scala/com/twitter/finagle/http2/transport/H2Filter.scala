package com.twitter.finagle.http2.transport

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext}
import io.netty.handler.codec.http2.Http2Frame
import io.netty.util.ReferenceCountUtil


/**
 * A handler to swallow inbound H2 frames. Useful for the boundaries between h2 and h1 portions
 * of the finagle-http2 pipeline.
 */
@Sharable
private[http2] object H2Filter extends ChannelDuplexHandler {
  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case frame: Http2Frame =>
      ReferenceCountUtil.release(frame)
    case _ =>
      super.channelRead(ctx, msg)
  }

  override def exceptionCaught(
    ctx: ChannelHandlerContext,
    cause: Throwable
  ): Unit = {
    // Swallowed so as to not bork the parent pipeline. This includes
    // GOAWAY messages.
    ()
  }
}
