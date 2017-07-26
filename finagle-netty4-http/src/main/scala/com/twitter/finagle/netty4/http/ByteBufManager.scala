package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.codec.ChannelBufferUsageTracker
import io.netty.buffer.ByteBuf
import io.netty.channel._

private[http] class ByteBufManager(usageTracker: ChannelBufferUsageTracker)
    extends ChannelDuplexHandler {

  private[this] var bufferUsage = 0L

  override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
    clearBufferUsage()
    super.write(ctx, msg, promise)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    msg match {
      case bb: ByteBuf => increaseBufferUsage(bb.capacity)
      case _ => ()
    }
    super.channelRead(ctx, msg)
  }

  override def close(ctx: ChannelHandlerContext, future: ChannelPromise): Unit = {
    clearBufferUsage()
    super.close(ctx, future)
  }

  private[this] def increaseBufferUsage(size: Long) = {
    // Don't change the order of the following statements, as usageTracker may throw an exception.
    usageTracker.increase(size)
    bufferUsage += size
  }

  private[this] def clearBufferUsage() = {
    // Don't change the order of the following statements, as usageTracker may throw an exception.
    usageTracker.decrease(bufferUsage)
    bufferUsage = 0
  }
}
