package com.twitter.finagle.stats

import com.twitter.finagle.util.LoadService

/**
 * A [[com.twitter.finagle.stats.StatsReceiver]] that loads
 * all service-loadable receivers and broadcasts stats to them.
 */
object LoadedStatsReceiver extends {

  /**
   * Mutating this value at runtime after it has been initialized should be done
   * with great care. If metrics have been created using the prior
   * [[StatsReceiver]], updates to those metrics may not be reflected in the
   * [[StatsReceiver]] that replaces it. In addition, histograms created with
   * the prior [[StatsReceiver]] will not be available.
   */
  @volatile var self: StatsReceiver = BroadcastStatsReceiver(LoadService[StatsReceiver]())
} with StatsReceiverProxy

/**
 * A [[com.twitter.finagle.stats.HostStatsReceiver]] that loads
 * all service-loadable receivers and broadcasts stats to them.
 */
object LoadedHostStatsReceiver extends {
  @volatile var self: StatsReceiver = BroadcastStatsReceiver(LoadService[HostStatsReceiver]())
} with HostStatsReceiver

/**
 * A "default" StatsReceiver loaded by Finagle's
 * [[com.twitter.finagle.util.LoadService]] mechanism.
 */
object DefaultStatsReceiver extends StatsReceiverProxy {
  def self: StatsReceiver = LoadedStatsReceiver
  override def repr: DefaultStatsReceiver.type = this

  def get: StatsReceiver = this
}

/**
 * A global StatsReceiver for generic finagle metrics.
 */
private[finagle] object FinagleStatsReceiver extends StatsReceiverProxy {
  val self: StatsReceiver = LoadedStatsReceiver.scope("finagle")
  override def repr: FinagleStatsReceiver.type = this

  def get: StatsReceiver = this
}

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
