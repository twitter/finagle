package com.twitter.finagle.stats

import com.twitter.common.metrics.{MetricCollisionException, Metrics}
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
    // gauges are weakly referenced by the registry so we need to keep a strong reference
    val g = rootReceiver.addGauge("my_gauge")(x)
    assert(readInRoot("my_gauge") === x)
  }

  test("cumulative gauge is working") {
    val x = 1
    val y = 2
    val z = 3
    val g1 = rootReceiver.addGauge("my_cumulative_gauge")(x)
    val g2 = rootReceiver.addGauge("my_cumulative_gauge")(y)
    val g3 = rootReceiver.addGauge("my_cumulative_gauge")(z)
    assert(readInRoot("my_cumulative_gauge") === x + y + z)
  }

  test("Ensure that we throw an exception with a counter and a gauge when rollup collides") {
    val sr = new RollupStatsReceiver(rootReceiver)
    sr.counter("a", "b", "c").incr()
    intercept[MetricCollisionException] {
      sr.addGauge("a", "b", "d") { 3 }
    }
  }

  test("Ensure that we throw an exception when rollup collides via scoping") {
    val sr = new RollupStatsReceiver(rootReceiver)
    val newSr = sr.scope("a").scope("b")
    newSr.counter("c").incr()
    intercept[MetricCollisionException] {
      newSr.addGauge("d") { 3 }
    }
  }

  test("store and read counter into the root StatsReceiver") {
    rootReceiver.counter("my_counter").incr()
    assert(readInRoot("my_counter") === 1)
  }

  test("separate gauge/stat/metric between detached Metrics and root Metrics") {
    val detachedReceiver = new MetricsStatsReceiver(Metrics.createDetached())
    val g1 = detachedReceiver.addGauge("xxx")(1.0f)
    val g2 = rootReceiver.addGauge("xxx")(2.0f)
    assert(read(detachedReceiver, "xxx") != read(rootReceiver, "xxx"))
  }
}
