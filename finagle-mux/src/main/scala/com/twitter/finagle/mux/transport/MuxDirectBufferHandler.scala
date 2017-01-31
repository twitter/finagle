package com.twitter.finagle.mux.transport

import com.twitter.finagle.mux.transport.Message.Types
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * Handler for managing framed inbound Mux buffers. Distinguishes itself from the
 * [[com.twitter.finagle.netty4.channel.DirectToHeapInboundHandler]] by excluding control
 * messages from copying and putting the onus on the Mux implementation for releasing them.
 *
 * @note Must be installed after the LengthFieldBasedFrameDecoder instance.
 */
@Sharable
private object MuxDirectBufferHandler extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case bb: ByteBuf if bb.isDirect =>
      if (bb.readableBytes() < 4) {
        // see @note above, we expect framed messages
        bb.release()
        throw BadMessageException(s"saw message with fewer than 4 readable bytes: $msg")
      }

      val idx = bb.readerIndex
      val header = bb.readInt() // we don't #slice to spare the object allocation
      bb.readerIndex(idx)
      val typ = Message.Tags.extractType(header)

      val res: ByteBuf =
        if (Types.isRefCounted(typ)) bb
        else {
          val heapBuf = ctx.alloc.heapBuffer(bb.readableBytes, bb.capacity)
          heapBuf.writeBytes(bb)
          bb.release()
          heapBuf
        }

      ctx.fireChannelRead(res)

    case _ =>
      ctx.fireChannelRead(msg)
  }
}
