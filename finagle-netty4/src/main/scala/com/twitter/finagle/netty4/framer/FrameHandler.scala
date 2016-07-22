package com.twitter.finagle.netty4.framer

import com.twitter.finagle.Failure
import com.twitter.finagle.framer.Framer
import com.twitter.io.Buf
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * The frame handler frames `Buf` pipeline messages, so it must be installed
 * after the handler which converts messages to `Buf`.
 */
private[netty4] class FrameHandler(framer: Framer) extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case buf: Buf =>
      var idx = 0
      val frames = framer(buf)
      while (idx < frames.length) {
        ctx.fireChannelRead(frames(idx))
        idx += 1
      }

    case _ =>
      ctx.fireExceptionCaught(Failure(
        s"FrameHandler saw non-Buf message: ${msg.toString}"))
  }
}
