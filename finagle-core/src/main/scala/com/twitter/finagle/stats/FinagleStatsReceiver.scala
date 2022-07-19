package com.twitter.finagle.stats

import com.twitter.finagle.util.LoadService

/**
 * A global StatsReceiver for generic finagle metrics.
 */
private[finagle] object FinagleStatsReceiver extends StatsReceiverProxy {

  protected val self: StatsReceiver = new FinagleStatsReceiverImpl(LoadedStatsReceiver)

  override def repr: FinagleStatsReceiver.type = this

  def get: StatsReceiver = this
}

/**
 * A [[com.twitter.finagle.stats.HostStatsReceiver]] that loads
 * all service-loadable receivers and broadcasts stats to them.
 */
object LoadedHostStatsReceiver extends {
  @volatile var self: StatsReceiver = BroadcastStatsReceiver(LoadService[HostStatsReceiver]())
} with HostStatsReceiver
