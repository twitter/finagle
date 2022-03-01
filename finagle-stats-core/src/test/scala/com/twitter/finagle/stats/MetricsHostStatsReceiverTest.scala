package com.twitter.finagle.stats

import org.scalatest.funsuite.AnyFunSuite

class MetricsHostStatsReceiverTest extends AnyFunSuite {

  private[this] def notNull(n: Number): Number = {
    if (n == null) throw null
    else n
  }

  val hostStatsReceiver = new MetricsHostStatsReceiver()

  def readHostStatsReceiverGauge(name: String): Number =
    notNull(hostStatsReceiver.registry.gauges.get(name))

  def readHostStatsReceiverCounter(name: String): Number =
    notNull(hostStatsReceiver.registry.counters.get(name))

  def readUnderlyingStatsReceiverGauge(name: String): Number =
    notNull(hostStatsReceiver.self.registry.gauges.get(name))

  def readUnderlyingStatsReceiverCounter(name: String): Number =
    notNull(hostStatsReceiver.self.registry.counters.get(name))

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
