package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class MetricsStatsReceiverTest extends FunSuite {
  private[this] val rootReceiver = new MetricsStatsReceiver()

  private[this] def read(metrics: MetricsStatsReceiver, name: String): Number =
    metrics.registry.sample().get(name)

  private[this] def readInRoot(name: String) = read(rootReceiver, name)

  test("MetricsStatsReceiver should store and read gauge into the root StatsReceiver") {
    val x = 1.5f
    rootReceiver.addGauge("my_gauge")(x)
    assert(readInRoot("my_gauge") === x)
  }

  test("cumulative gauge is working") {
    val x = 1
    val y = 2
    val z = 3
    rootReceiver.addGauge("my_cumulative_gauge")(x)
    rootReceiver.addGauge("my_cumulative_gauge")(y)
    rootReceiver.addGauge("my_cumulative_gauge")(z)
    assert(readInRoot("my_cumulative_gauge") === x + y + z)
  }

  test("store and read counter into the root StatsReceiver") {
    rootReceiver.counter("my_counter").incr()
    assert(readInRoot("my_counter") === 1)
  }

  test("separate gauge/stat/metric between detached Metrics and root Metrics") {
    val detachedReceiver = new MetricsStatsReceiver(Metrics.createDetached())
    detachedReceiver.addGauge("xxx")(1.0f)
    rootReceiver.addGauge("xxx")(2.0f)
    assert(read(detachedReceiver, "xxx") != read(rootReceiver, "xxx"))
  }
}
