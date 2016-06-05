package com.twitter.finagle.stats

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class MetricsHostStatsReceiverTest extends FunSuite {
  val hostStatsReceiver = new MetricsHostStatsReceiver()

  def readHostStatsReceiver(name: String): Number =
    hostStatsReceiver.registry.sample().get(name)

  def readUnderlyingStatsReceiver(name: String): Number =
    hostStatsReceiver.self.registry.sample().get(name)

  test("MetricsHostStatsReceiver is a proxy of underlying MetricsStatsReceiver") {
    hostStatsReceiver.addGauge("my_cumulative_gauge")(1)
    hostStatsReceiver.addGauge("my_cumulative_gauge")(2)
    hostStatsReceiver.counter("my_counter").incr()

    assert(
      readHostStatsReceiver("my_cumulative_gauge") ==
        readUnderlyingStatsReceiver("my_cumulative_gauge"))
    assert(
      readHostStatsReceiver("my_counter") ==
        readUnderlyingStatsReceiver("my_counter"))
  }
}
