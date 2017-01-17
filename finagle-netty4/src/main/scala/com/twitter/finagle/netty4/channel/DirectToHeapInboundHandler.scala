package com.twitter.finagle.netty4.channel

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * An inbound channel handler that copies direct byte buffers onto the JVM heap
 * and gives them a deterministic lifecycle.
 *
 * @note If your protocol manages ref-counting or if you are delegating ref-counting
 *       to application space you don't need this handler in your pipeline. Every
 *       other use case needs this handler or you will with very high probability
 *       incur a direct buffer leak.
 */
@Sharable
object DirectToHeapInboundHandler extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case bb: ByteBuf if bb.isDirect =>
      val heapBuf = ctx.alloc().heapBuffer(bb.readableBytes, bb.capacity)
      heapBuf.writeBytes(bb)

      bb.release()
      ctx.fireChannelRead(heapBuf)
    case _ => ctx.fireChannelRead(msg)
  }
}
