package com.twitter.finagle.netty4.decoder

import com.twitter.finagle.Failure
import com.twitter.finagle.decoder.Decoder
import com.twitter.io.Buf
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
 * The decoder handler decodes `Buf`s into pipeline messages, so it must be installed
 * after the handler which converts messages to `Buf`.
 */
private[finagle] class DecoderHandler[T](decoder: Decoder[T]) extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case buf: Buf =>
      var idx = 0
      val messages = decoder(buf)
      while (idx < messages.length) {
        ctx.fireChannelRead(messages(idx))
        idx += 1
      }

    case _ =>
      ctx.fireExceptionCaught(Failure(s"DecoderHandler saw non-Buf message: ${msg.toString}"))
  }
}
