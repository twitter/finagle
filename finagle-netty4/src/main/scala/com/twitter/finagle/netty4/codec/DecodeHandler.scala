package com.twitter.finagle.netty4.codec

import com.twitter.finagle.codec.FrameDecoder
import com.twitter.finagle.netty4.ByteBufAsBuf
import com.twitter.logging.Logger
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.AttributeKey

private[codec] object DecodeHandler {
  val log = Logger(getClass.getName)
}

/**
 * The decode handler applies a [[com.twitter.finagle.codec.FrameDecoder]]
 * instance to inbound pipeline messages. Consequently it should be installed
 * before all pipeline handlers which expect an `In`-typed message.
 */
private[netty4] class DecodeHandler[In](
    decoderFactory: () => FrameDecoder[In])
  extends ChannelInboundHandlerAdapter {
  import DecodeHandler.log

  private[this] val DecoderKey = AttributeKey.valueOf[FrameDecoder[In]]("frame_decoder")

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.attr(DecoderKey).set(decoderFactory())
    super.channelActive(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case bb: ByteBuf =>
      var idx = 0
      val frames = ctx.attr(DecoderKey).get.apply(ByteBufAsBuf.Owned(bb))
      while (idx < frames.length) {
        ctx.fireChannelRead(frames(idx))
        idx += 1
      }

    case _ =>
      log.warning(s"DecodeHandler saw non-ByteBuf message: ${msg.toString}")
  }
}
