package com.twitter.finagle.netty4.channel

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * An inbound channel handler that copies direct byte buffers onto heap.
 *
 * The intention of this is to make Netty 4 behave as Netty 3 w.r.t. direct byte buffers
 * and offload freeing native memory from the GC cycle.
 *
 * See CSL-3027 for more details.
 */
@Sharable
private[netty4] object DirectToHeapInboundHandler extends ChannelInboundHandlerAdapter  {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case bb: ByteBuf if bb.isDirect =>
      val heapBuf = ctx.alloc().heapBuffer(bb.readableBytes, bb.capacity)
      heapBuf.writeBytes(bb)

      bb.release()
      ctx.fireChannelRead(heapBuf)
    case _ => ctx.fireChannelRead(msg)
  }
}
