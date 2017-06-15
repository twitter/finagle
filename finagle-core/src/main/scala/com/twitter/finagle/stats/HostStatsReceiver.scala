package com.twitter.finagle.stats

/**
 * A StatsReceiver type that's used for per host stats only. The underlying
 * implementation can be any StatsReceiver. The type is used as a marker.
 */
trait HostStatsReceiver extends StatsReceiverProxy

class InMemoryHostStatsReceiver extends HostStatsReceiver {
  val self: InMemoryStatsReceiver = new InMemoryStatsReceiver
}
