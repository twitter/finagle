package com.twitter.finagle.netty4.channel

import io.netty.buffer.{ByteBuf, ByteBufHolder, EmptyByteBuf, Unpooled}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * An inbound channel handler that copies byte buffers onto the JVM heap
 * and gives them a deterministic lifecycle. This handler also makes sure to
 * use the unpooled byte buffer (regardless of what channel's allocator is) as
 * its destination thereby defining a clear boundaries between pooled and unpooled
 * environments.
 *
 * @note If the input buffer is not readable it's still guaranteed to be released
 *       and replaced with EmptyByteBuf.
 *
 * @note This handler recognizes both ByteBuf's and ByteBufHolder's (think of HTTP
 *       messages extending ByteBufHolder's in Netty).
 *
 * @note If your protocol manages ref-counting or if you are delegating ref-counting
 *       to application space you don't need this handler in your pipeline. Every
 *       other use case needs this handler or you will with very high probability
 *       incur a direct buffer leak.
 */
@Sharable
object AnyToHeapInboundHandler extends ChannelInboundHandlerAdapter {
  private[this] final def copyOnHeapAndRelease(bb: ByteBuf): ByteBuf = {
    try {
      if (bb.readableBytes > 0) Unpooled.buffer(bb.readableBytes, bb.capacity).writeBytes(bb)
      else Unpooled.EMPTY_BUFFER
    } finally bb.release()
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case bb: ByteBuf =>
      ctx.fireChannelRead(copyOnHeapAndRelease(bb))

    // This case is special since it helps to avoid unnecessary `replace`
    // when the underlying content is already `EmptyByteBuffer`.
    case bbh: ByteBufHolder if bbh.content.isInstanceOf[EmptyByteBuf] =>
      ctx.fireChannelRead(bbh)

    case bbh: ByteBufHolder =>
      val onHeapContent = copyOnHeapAndRelease(bbh.content)
      ctx.fireChannelRead(bbh.replace(onHeapContent))

    case _ =>
      ctx.fireChannelRead(msg)
  }
}
