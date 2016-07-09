package com.twitter.finagle.netty3.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.codec.FrameDecoder
import com.twitter.io.Buf
import org.jboss.netty.channel._

/**
 * Frames Bufs into protocol frames
 */
private[finagle] class FrameDecoderHandler(
  framer: FrameDecoder[Buf]) extends SimpleChannelUpstreamHandler {

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    e.getMessage match {
      case buf: Buf =>
        val frames: IndexedSeq[Buf] = framer(buf)
        var i = 0
        while (i < frames.length) {
          Channels.fireMessageReceived(ctx, frames(i))
          i += 1
        }
      case msg => Channels.fireExceptionCaught(ctx, Failure(
        "unexpected type when framing Buf." +
          s"Expected Buf, got ${msg.getClass.getSimpleName}."))
    }
}