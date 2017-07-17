package com.twitter.finagle.netty4.encoder

import com.twitter.finagle.netty4.codec.BufCodec
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelOutboundHandlerAdapter, ChannelPromise}

/**
 * A Buf -> ByteBuf encoder.
 */
@Sharable
private[finagle] object BufEncoder extends ChannelOutboundHandlerAdapter {

  override def write(ctx: ChannelHandlerContext, msg: Any, p: ChannelPromise): Unit =
    BufCodec.write(ctx, msg, p)
}
