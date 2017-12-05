package com.twitter.finagle.thrift

import com.twitter.finagle.stats.{Counter, StatsReceiver}

object ThriftMethodStats {

  def apply(stats: StatsReceiver): ThriftMethodStats =
    ThriftMethodStats(
      stats.counter("requests"),
      stats.counter("success"),
      stats.counter("failures"),
      stats.scope("failures")
    )
}

case class ThriftMethodStats(
  requestsCounter: Counter,
  successCounter: Counter,
  failuresCounter: Counter,
  failuresScope: StatsReceiver
)
