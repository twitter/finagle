package com.twitter.finagle.netty4.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.codec.FrameEncoder
import com.twitter.finagle.netty4.BufAsByteBuf
import com.twitter.io.Buf
import com.twitter.util.NonFatal
import io.netty.channel.{ChannelPromise, ChannelHandlerContext, ChannelOutboundHandlerAdapter}
import io.netty.channel.ChannelHandler.Sharable

/**
 * A netty4 channel handler which encodes outbound `Out`-typed messages into
 * [[io.netty.buffer.ByteBuf ByteBufs]].
 *
 * @note for positioning in the pipeline, this handler constitutes the boundary
 *       between an `Out`-typed application frame and netty's ByteBufs. Install
 *       outbound handlers around it accordingly.
 */
@Sharable
private[netty4] class EncodeHandler[Out](frameEncoder: FrameEncoder[Out]) extends ChannelOutboundHandlerAdapter {

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
    val encoded =
      try { frameEncoder(msg.asInstanceOf[Out]) } catch {
        case NonFatal(e) =>
          ctx.pipeline.fireExceptionCaught(Failure("encoding failure", e))
          Buf.Empty
      }

    if (!encoded.isEmpty) super.write(ctx, BufAsByteBuf.Owned(encoded), promise)
  }
}
