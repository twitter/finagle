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
   * [[StatsReceiver]] that replaces it.
   */
  @volatile var self: StatsReceiver = {
    val receivers = LoadService[StatsReceiver]()
    BroadcastStatsReceiver(receivers)
  }
} with StatsReceiverProxy

/**
 * A "default" StatsReceiver loaded by Finagle's
 * [[com.twitter.finagle.util.LoadService]] mechanism.
 */
object DefaultStatsReceiver extends {
  val self: StatsReceiver = LoadedStatsReceiver
} with StatsReceiverProxy {
  val get = this
}

/**
 * A global StatsReceiver for generic finagle metrics.
 */
private[finagle] object FinagleStatsReceiver extends {
  val self: StatsReceiver = LoadedStatsReceiver.scope("finagle")
} with StatsReceiverProxy {
  val get: StatsReceiver = this
}

/**
 * A client-specific StatsReceiver. All stats recorded using this receiver
 * are prefixed with the string "clnt" by default.
 */
object ClientStatsReceiver extends StatsReceiverProxy {
  @volatile private[this] var _self: StatsReceiver = LoadedStatsReceiver.scope("clnt")
  def self: StatsReceiver = _self
  def setRootScope(rootScope: String) {
    _self = LoadedStatsReceiver.scope(rootScope)
  }
}

/**
 * A server-specific StatsReceiver. All stats recorded using this receiver
 * are prefixed with the string "srv" by default.
 */
object ServerStatsReceiver extends StatsReceiverProxy {
  @volatile private[this] var _self: StatsReceiver = LoadedStatsReceiver.scope("srv")
  def self: StatsReceiver = _self
  def setRootScope(rootScope: String) {
    _self = LoadedStatsReceiver.scope(rootScope)
  }
}

/**
 * A [[com.twitter.finagle.stats.HostStatsReceiver]] that loads
 * all service-loadable receivers and broadcasts stats to them.
 */
object LoadedHostStatsReceiver extends HostStatsReceiver {
  @volatile var _self: StatsReceiver = {
    val receivers = LoadService[HostStatsReceiver]()
    BroadcastStatsReceiver(receivers)
  }
  def self = _self
}
