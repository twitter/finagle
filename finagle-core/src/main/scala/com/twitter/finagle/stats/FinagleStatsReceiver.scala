package com.twitter.finagle.stats

import com.twitter.finagle.util.LoadService

/**
 * A global StatsReceiver for generic finagle metrics.
 */
private[finagle] object FinagleStatsReceiver extends StatsReceiverProxy {
  val self: StatsReceiver = LoadedStatsReceiver.scope("finagle")
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

/**
 * A client-specific StatsReceiver. All stats recorded using this receiver
 * are prefixed with the string "clnt" by default.
 */
object ClientStatsReceiver extends StatsReceiverProxy {
  @volatile protected var self: StatsReceiver =
    RoleConfiguredStatsReceiver(LoadedStatsReceiver.scope("clnt"), Client)

  def setRootScope(rootScope: String): Unit = {
    self = RoleConfiguredStatsReceiver(LoadedStatsReceiver.scope(rootScope), Client)
  }

  override def repr: ClientStatsReceiver.type = this

  def get: StatsReceiver = this
}

/**
 * A server-specific StatsReceiver. All stats recorded using this receiver
 * are prefixed with the string "srv" by default.
 */
object ServerStatsReceiver extends StatsReceiverProxy {
  @volatile protected var self: StatsReceiver =
    RoleConfiguredStatsReceiver(LoadedStatsReceiver.scope("srv"), Server)

  def setRootScope(rootScope: String): Unit = {
    self = RoleConfiguredStatsReceiver(LoadedStatsReceiver.scope(rootScope), Server)
  }

  override def repr: ServerStatsReceiver.type = this

  def get: StatsReceiver = this
}
