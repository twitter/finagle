package com.twitter.finagle.stats

/**
 * Interface used via the LoadService mechanism to obtain a stats
 * registry used in MetricsRegistry. This avoids MetricsHandler,
 * (which uses a stats registry), in twitter-server, having a
 * dependency on finagle-stats.
 */
private[twitter] trait StatsRegistry {
  def getStats(): Map[String, StatEntry]
}

/**
 * Interface to allow MetricsHandler (in twitter-server) to
 * use metrics from MetricsRegistry (in finagle-stats)
 * without twitter-server having a dependency on finagle-stats.
 */
private[twitter] trait StatEntry {
  val value: Double
  val totalValue: Double
}
