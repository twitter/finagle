package com.twitter.finagle.netty4.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.codec.FrameEncoder
import com.twitter.finagle.netty4.BufAsByteBuf
import com.twitter.util.NonFatal
import io.netty.channel.{ChannelPromise, ChannelHandlerContext, ChannelOutboundHandlerAdapter}
import io.netty.channel.ChannelHandler.Sharable

/**
 * A netty4 channel handler which encodes outbound `Out`-typed messages into
 * [[io.netty.buffer.ByteBuf ByteBufs]].
 *
 * @note For positioning in the pipeline, this handler constitutes the boundary
 *       between an `Out`-typed application frame and netty's ByteBufs. Install
 *       outbound handlers around it accordingly.
 *
 * @note An empty `Buf` returned form [[FrameEncoder]] won't be written into the pipeline and the
 *       promise associated with a write will be satisfied.
 */
@Sharable
private[netty4] class EncodeHandler[Out](frameEncoder: FrameEncoder[Out]) extends ChannelOutboundHandlerAdapter {

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
    try {
      val encoded = frameEncoder(msg.asInstanceOf[Out])
      if (!encoded.isEmpty) super.write(ctx, BufAsByteBuf.Owned(encoded), promise)
      else promise.setSuccess()
    } catch {
      // We don't fire an exception into the pipeline when an encoding failure
      // occurs assuming that a caller of `channel.write()` will observe a failure on
      // a returned write-promise.
      case NonFatal(e) => promise.setFailure(Failure("encoding failure", e))
    }
  }
}
