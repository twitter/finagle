package com.twitter.finagle.stats

/**
 * A client-specific StatsReceiver. All stats recorded using this receiver
 * are prefixed with the string "clnt" by default.
 */
object ClientStatsReceiver extends StatsReceiverProxy {
  @volatile protected var self: StatsReceiver = _
  setRootScope("clnt")

  def setRootScope(rootScope: String): Unit = {
    self = RoleConfiguredStatsReceiver(LoadedStatsReceiver.scope(rootScope), SourceRole.Client)
  }

  override def repr: ClientStatsReceiver.type = this

  def get: StatsReceiver = this
}
