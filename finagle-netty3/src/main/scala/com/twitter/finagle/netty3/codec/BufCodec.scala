package com.twitter.finagle.netty3.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

/**
 * A ChannelBuffer <-> Buf codec.
 *
 * @note this is intended to be installed after framing in a Finagle
 * protocol implementation such that the `In` and `Out` types for the
 * StackClient or StackServer will be [[Buf]].
 */
private[finagle] class BufCodec extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    e.getMessage match {
      case b: Buf => Channels.write(ctx, e.getFuture, BufChannelBuffer(b))
      case typ =>
        e.getFuture.setFailure(
          Failure(s"unexpected type ${typ.getClass.getSimpleName} when encoding to ChannelBuffer")
        )
    }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    e.getMessage match {
      case cb: ChannelBuffer => Channels.fireMessageReceived(ctx, ChannelBufferBuf.Owned(cb))
      case typ =>
        Channels.fireExceptionCaught(
          ctx,
          Failure(s"unexpected type ${typ.getClass.getSimpleName} when encoding to Buf")
        )
    }
}
