package com.twitter.finagle.thrift

import com.twitter.finagle.stats.{Counter, LazyStatsReceiver, NullStatsReceiver, StatsReceiver}

object ThriftMethodStats {

  def apply(stats: StatsReceiver): ThriftMethodStats = {
    val wrapped = new LazyStatsReceiver(stats)
    ThriftMethodStats(
      wrapped.counter("requests"),
      wrapped.counter("success"),
      wrapped.counter("failures"),
      wrapped.scope("failures")
    )
  }

  private[this] val NullThriftMethodStats = apply(NullStatsReceiver)

  /**
   * An instance of [[ThriftMethodStats]] that is backed by a
   * `NullStatsReceiver`.
   *
   * This can be used as a sentinel instance where everything is a no-op.
   */
  def Null: ThriftMethodStats = NullThriftMethodStats
}

case class ThriftMethodStats private (
  requestsCounter: Counter,
  successCounter: Counter,
  failuresCounter: Counter,
  failuresScope: StatsReceiver)
