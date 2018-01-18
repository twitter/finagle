package com.twitter.finagle.netty4.http.handler

import io.netty.buffer.{EmptyByteBuf, Unpooled}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.HttpContent
import io.netty.util.ReferenceCounted

/**
 * An inbound channel handler that copies HTTP contents onto the JVM heap
 * and gives them a deterministic lifecycle. This handler also makes sure to
 * use the unpooled byte buffer (regardless of what channel's allocator is) as
 * its destination thereby defining a clear boundaries between pooled and unpooled
 * environments.
 *
 * @note If the input buffer/content is not readable it's still guaranteed to be released
 *       and replaced with EmptyByteBuf.
 */
@Sharable
private[finagle] object UnpoolHttpHandler extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    // This case is special since it helps to avoid unnecessary `replace`
    // when the underlying content is already `EmptyByteBuffer`.
    case hc: HttpContent if hc.content.isInstanceOf[EmptyByteBuf] =>
      ctx.fireChannelRead(hc)

    case hc: HttpContent =>
      val onHeapContent =
        try Unpooled.buffer(hc.content.readableBytes, hc.content.capacity).writeBytes(hc.content)
        finally hc.content.release()

      ctx.fireChannelRead(hc.replace(onHeapContent))

    case _ =>
      assert(!msg.isInstanceOf[ReferenceCounted])
      ctx.fireChannelRead(msg)
  }
}
