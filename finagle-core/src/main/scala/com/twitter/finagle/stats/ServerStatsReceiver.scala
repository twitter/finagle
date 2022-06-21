package com.twitter.finagle.stats

/**
 * A server-specific StatsReceiver. All stats recorded using this receiver
 * are prefixed with the string "srv" by default.
 */
object ServerStatsReceiver extends StatsReceiverProxy {
  @volatile protected var self: StatsReceiver = _
  setRootScope("srv")

  def setRootScope(rootScope: String): Unit = {
    self = RoleConfiguredStatsReceiver(LoadedStatsReceiver.scope(rootScope), SourceRole.Server)
  }

  override def repr: ServerStatsReceiver.type = this

  def get: StatsReceiver = this
}
