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
  @volatile protected var self: StatsReceiver = _
  setRootScope("clnt")

  def setRootScope(rootScope: String): Unit = {
    self = RoleConfiguredStatsReceiver(transformLoadedStatsReceiver(rootScope), Client)
  }

  override def repr: ClientStatsReceiver.type = this

  def get: StatsReceiver = this

  private[this] def transformLoadedStatsReceiver(rootScope: String): StatsReceiver = {
    // The hierarchical structure is flexible but we're not allowing changes to the
    // dimensional representation at this time.
    LoadedStatsReceiver
      .hierarchicalScope(rootScope)
      .dimensionalScope("rpc")
      .dimensionalScope("finagle")
      .dimensionalScope("client")
      .label("rpc_system", "finagle")
      .label("implementation", "finagle")
  }
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
