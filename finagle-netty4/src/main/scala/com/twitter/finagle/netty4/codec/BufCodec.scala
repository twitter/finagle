package com.twitter.finagle.netty4.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.io.Buf
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import io.netty.channel.ChannelHandler.Sharable

/**
 * A ByteBuffer <-> Buf codec.
 *
 * @note this handler guarantees to only emit instances of [[Buf.ByteArray]]
 *       as well as to release the inbound [[ByteBuf]].
 *
 * @note this is intended to be installed after framing in a Finagle
 * protocol implementation such that the `In` and `Out` types for the
 * StackClient or StackServer will be [[Buf]].
 */
@Sharable
private[finagle] object BufCodec extends ChannelDuplexHandler {

  // The key to use when inserting `BufCodec` into a Netty `ChannelPipeline`.
  val Key: String = "bufCodec"

  override def write(ctx: ChannelHandlerContext, msg: Any, p: ChannelPromise): Unit =
    msg match {
      // We bypass Java NIO byte buffers if they're already direct.
      case Buf.ByteBuffer(buf) if buf.isDirect =>
        ctx.write(Unpooled.wrappedBuffer(buf), p)

      // We're writing `Buf`s directly into off-heap buffers because:
      //
      //  1. We want to stop referencing heap-backed buffers (preventing them from being tenured)
      //     and take an advantage of pooling as early as possible.
      //
      //  2. This allows us to eliminate one memory copy when `Buf` is composite (literally,
      //     every Finagle message). Given that direct buffers are ready to be sent over the
      //     wire, writing composite `Buf`s into direct byte buffers means skipping an interim
      //     on-heap copy needed to convert on-heap composite into on-heap regular buffer
      //     (when `BufAsByteBuf` is used).
      //
      case buf: Buf =>
        val length = buf.length
        val byteBuf = ctx.alloc().directBuffer(length)

        // We need to advance the `writerIndex` manually since `byteBuf.nioBuffer` indices are
        // independent from its parent buffer's indices. It's also important to do that prior
        // writing such that the destination nio buffer will have a proper `capacity`.
        byteBuf.writerIndex(length)
        buf.write(byteBuf.nioBuffer())

        ctx.write(byteBuf, p)

      case typ =>
        p.setFailure(
          Failure(s"unexpected type ${typ.getClass.getSimpleName} when encoding to ByteBuf")
        )
    }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit =
    msg match {
      case bb: ByteBuf =>
        val result =
          try ByteBufConversion.readByteBufToBuf(bb)
          finally bb.release()

        ctx.fireChannelRead(result)

      case typ =>
        ctx.fireExceptionCaught(
          Failure(s"unexpected type ${typ.getClass.getSimpleName} when encoding to Buf")
        )
    }
}
