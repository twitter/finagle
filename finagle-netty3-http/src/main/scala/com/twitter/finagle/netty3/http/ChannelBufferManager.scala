package com.twitter.finagle.netty3.http

import com.twitter.finagle.http.codec.ChannelBufferUsageTracker
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

private class ChannelBufferManager(usageTracker: ChannelBufferUsageTracker)
    extends SimpleChannelHandler {
  private[this] var bufferUsage = 0L

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case buffer: ChannelBuffer => increaseBufferUsage(buffer.capacity())
      case _ => ()
    }

    super.messageReceived(ctx, e)
  }

  override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
    clearBufferUsage()

    super.writeComplete(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    clearBufferUsage()

    super.channelClosed(ctx, e)
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
