package com.twitter.finagle.thrift.transport.netty4

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * ThriftChannel decoder: this simply converts the underlying
 * ByteBuf (which have been deframed) into byte arrays.
 */
@Sharable
private[netty4] object ThriftByteBufToArrayDecoder extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = msg match {
    case buffer: ByteBuf =>
      // toArray takes ownership of the buffer
      val array = toArray(buffer)
      ctx.fireChannelRead(array)

    case _ => throw new IllegalArgumentException("no byte buffer")
  }

  // takes ownership of the passed `ByteBuf`
  private def toArray(buffer: ByteBuf): Array[Byte] = {
    val array = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(array)
    buffer.release() // If you love it, set it free.
    array
  }
}
