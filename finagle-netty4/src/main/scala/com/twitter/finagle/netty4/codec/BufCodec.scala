package com.twitter.finagle.netty4.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.netty4.{BufAsByteBuf, ByteBufAsBuf}
import com.twitter.io.Buf
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelPromise, ChannelHandlerContext, ChannelDuplexHandler}

/**
 * A ByteBuffer <-> Buf codec.
 *
 * @note this is intended to be installed after framing in a Finagle
 * protocol implementation such that the `In` and `Out` types for the
 * StackClient or StackServer will be [[Buf]].
 */
private[finagle] class BufCodec extends ChannelDuplexHandler {
  override def write(ctx: ChannelHandlerContext, msg: Any, p: ChannelPromise): Unit =
    msg match {
      case buf: Buf => ctx.write(BufAsByteBuf.Owned(buf), p)
      case typ => p.setFailure(Failure(
        s"unexpected type ${typ.getClass.getSimpleName} when encoding to ByteBuf"))
    }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit =
    msg match {
      case bb: ByteBuf => ctx.fireChannelRead(ByteBufAsBuf.Owned(bb))
      case typ => ctx.fireExceptionCaught(Failure(
          s"unexpected type ${typ.getClass.getSimpleName} when encoding to Buf"))
    }
}