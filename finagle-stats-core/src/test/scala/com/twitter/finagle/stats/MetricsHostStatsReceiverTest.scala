package com.twitter.finagle.stats

import org.scalatest.funsuite.AnyFunSuite

class MetricsHostStatsReceiverTest extends AnyFunSuite {
  val hostStatsReceiver = new MetricsHostStatsReceiver()

  def readHostStatsReceiverGauge(name: String): Number =
    hostStatsReceiver.registry.gauges.get(Seq(name))

  def readHostStatsReceiverCounter(name: String): Number =
    hostStatsReceiver.registry.counters.get(Seq(name))

  def readUnderlyingStatsReceiverGauge(name: String): Number =
    hostStatsReceiver.self.registry.gauges.get(Seq(name))

  def readUnderlyingStatsReceiverCounter(name: String): Number =
    hostStatsReceiver.self.registry.counters.get(Seq(name))

  test("MetricsHostStatsReceiver is a proxy of underlying MetricsStatsReceiver") {
    hostStatsReceiver.addGauge("my_cumulative_gauge")(1)
    hostStatsReceiver.addGauge("my_cumulative_gauge")(2)
    hostStatsReceiver.counter("my_counter").incr()

    assert(
      readHostStatsReceiverGauge("my_cumulative_gauge") ==
        readUnderlyingStatsReceiverGauge("my_cumulative_gauge")
    )
    assert(
      readHostStatsReceiverCounter("my_counter") ==
        readUnderlyingStatsReceiverCounter("my_counter")
    )
  }
}
