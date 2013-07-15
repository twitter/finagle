package com.twitter.finagle.stats

import com.twitter.finagle.util.LoadService

/**
 * A [[com.twitter.finagle.stats.StatsReceiver]] that loads
 * all service-loadable receivers and broadcasts stats to them.
 */
object LoadedStatsReceiver extends {
  @volatile var self: StatsReceiver = {
    val receivers = LoadService[StatsReceiver]()
    BroadcastStatsReceiver(receivers)
  }
} with StatsReceiverProxy
