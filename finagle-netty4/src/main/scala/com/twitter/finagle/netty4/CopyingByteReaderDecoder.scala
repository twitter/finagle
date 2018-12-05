package com.twitter.finagle.netty4

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

@Sharable
private[finagle] object CopyingByteReaderDecoder extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = msg match {
    case bb: ByteBuf =>
      ctx.fireChannelRead(new CopyingByteBufByteReader(bb))

    case other =>
      val exc = new IllegalStateException(s"Expected ByteBuf, found $other")
      ctx.fireExceptionCaught(exc)
  }
}
