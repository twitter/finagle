package com.twitter.finagle.http.codec

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.ChannelBufferUsageException
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.StorageUnit

/**
 * ChannelBufferUsageTracker tracks the channel buffer used by outstanding
 * requests. An exception will be thrown if the total size exceeds a limit.
 * ChannelBufferManager uses ChannelBufferUsageTracker.
 */
private[finagle] class ChannelBufferUsageTracker(
  limit: StorageUnit,
  statsReceiver: StatsReceiver = NullStatsReceiver) {
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
          + currentUsage + ", " + size + " vs. " + usageLimit + ")"
      )
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
          + size + " vs. " + currentUsage + ")"
      )
    } else {
      state.currentUsage -= size
    }
  }
}
