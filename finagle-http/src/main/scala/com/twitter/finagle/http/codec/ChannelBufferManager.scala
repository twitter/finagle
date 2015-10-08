package com.twitter.finagle.http.codec

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer

import com.twitter.finagle.ChannelBufferUsageException
import com.twitter.util.StorageUnit
import com.twitter.conversions.storage._

/**
 * ChannelBufferUsageTracker tracks the channel buffer used by outstanding
 * requests. An exception will be thrown if the total size exceeds a limit.
 * ChannelBufferManager uses ChannelBufferUsageTracker.
 */
class ChannelBufferUsageTracker(
  limit: StorageUnit,
  statsReceiver: StatsReceiver = NullStatsReceiver
) {
  private[this] object state {
    var currentUsage = 0L
    var maxUsage = 0L
    var usageLimit = limit
  }

  // It is probably not necessary to use synchronized methods here. We
  // can change this if there is a performance problem.
  private[this] val currentUsageStat =
    statsReceiver.addGauge("channel_buffer_current_usage") { currentUsage.inBytes }
  private[this] val maxUsageStat =
    statsReceiver.addGauge("channel_buffer_max_usage") { maxUsage.inBytes }

  def currentUsage: StorageUnit = synchronized { state.currentUsage.bytes }

  def maxUsage: StorageUnit = synchronized { state.maxUsage.bytes }

  def usageLimit(): StorageUnit = synchronized { state.usageLimit }

  def setUsageLimit(limit: StorageUnit) = synchronized { state.usageLimit = limit }

  def increase(size: Long) = synchronized {
    if (state.currentUsage + size > state.usageLimit.inBytes) {
      throw new ChannelBufferUsageException(
        "Channel buffer usage exceeded limit ("
        + currentUsage + ", " + size + " vs. " + usageLimit + ")")
    } else {
      state.currentUsage += size
      if (currentUsage > maxUsage)
        state.maxUsage = state.currentUsage
    }
  }

  def decrease(size: Long) = synchronized {
    if (state.currentUsage < size) {
      throw new ChannelBufferUsageException(
        "invalid ChannelBufferUsageTracker decrease operation ("
        + size + " vs. " + currentUsage + ")")
    } else {
      state.currentUsage -= size
    }
  }
}

private[http]
class ChannelBufferManager(usageTracker: ChannelBufferUsageTracker)
  extends SimpleChannelHandler
{
  private[this] var bufferUsage = 0L

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case buffer: ChannelBuffer => increaseBufferUsage(buffer.capacity())
      case _                     =>  ()
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

