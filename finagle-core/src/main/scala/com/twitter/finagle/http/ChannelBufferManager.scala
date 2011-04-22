package com.twitter.finagle.http

/**
 * ChannelBufferUsageTracker tracks the channel buffer used by outstanding requests. An exception
 * will be thrown if the total size exceeds a limit.
 * ChannelBufferManager uses ChannelBufferUsageTracker.
 */

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.ChannelBufferUsageException

class ChannelBufferUsageTracker(limit: Long, statsReceiver: StatsReceiver = NullStatsReceiver) {
  private[this] var currentUsage = 0L
  private[this] var maxUsage = 0L
  private[this] var usageLimit = limit

  // It is probably not necessary to use synchronized methods here. We can change this if there is a performance problem.
  private[this] val currentUsageStat = statsReceiver.addGauge("channel_buffer_current_usage") { currentUsage() }
  private[this] val maxUsageStat = statsReceiver.addGauge("channel_buffer_max_usage") { maxUsage() }

  def currentUsage(): Long = synchronized { currentUsage}

  def maxUsage(): Long = synchronized { maxUsage }

  def usageLimit(): Long = synchronized { usageLimit }

  def setUsageLimit(limit: Long) = synchronized { usageLimit = limit }

  def increase(size: Long) = synchronized {
    if (currentUsage + size > usageLimit) {
      throw new ChannelBufferUsageException(
        "Channel buffer usage exceeded limit (" + currentUsage + ", " + size + " vs. " + usageLimit + ")")
    } else {
      currentUsage += size
      if (currentUsage > maxUsage)
        maxUsage = currentUsage
    }
  }

  def decrease(size: Long) = synchronized {
    if (currentUsage < size) {
      throw new ChannelBufferUsageException(
        "invalid ChannelBufferUsageTracker decrease operation (" + size + " vs. " + currentUsage + ")")
    } else {
      currentUsage -= size
    }
  }
}

class ChannelBufferManager(usageTracker: ChannelBufferUsageTracker) extends SimpleChannelHandler {
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

