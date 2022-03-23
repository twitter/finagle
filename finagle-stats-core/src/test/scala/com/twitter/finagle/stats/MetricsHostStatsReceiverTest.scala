package com.twitter.finagle.stats

import com.twitter.finagle.stats.Helpers.get
import org.scalatest.funsuite.AnyFunSuite

class MetricsHostStatsReceiverTest extends AnyFunSuite {

  val hostStatsReceiver = new MetricsHostStatsReceiver()

  def readHostStatsReceiverGauge(name: String): Double =
    get(name, hostStatsReceiver.registry.gauges).value.doubleValue

  def readHostStatsReceiverCounter(name: String): Long =
    get(name, hostStatsReceiver.registry.counters).value

  def readUnderlyingStatsReceiverGauge(name: String): Double =
    get(name, hostStatsReceiver.self.registry.gauges).value.doubleValue

  def readUnderlyingStatsReceiverCounter(name: String): Long =
    get(name, hostStatsReceiver.self.registry.counters).value

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
