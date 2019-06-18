package com.twitter.finagle.thrift

import com.twitter.finagle.stats.{Counter, NullStatsReceiver, StatsReceiver}

object ThriftMethodStats {

  def apply(stats: StatsReceiver): ThriftMethodStats =
    ThriftMethodStats(
      stats.counter("requests"),
      stats.counter("success"),
      stats.counter("failures"),
      stats.scope("failures")
    )

  private[this] val NullThriftMethodStats = apply(NullStatsReceiver)

  /**
   * An instance of [[ThriftMethodStats]] that is backed by a
   * `NullStatsReceiver`.
   *
   * This can be used as a sentinel instance where everything is a no-op.
   */
  def Null: ThriftMethodStats = NullThriftMethodStats
}

case class ThriftMethodStats(
  requestsCounter: Counter,
  successCounter: Counter,
  failuresCounter: Counter,
  failuresScope: StatsReceiver)
