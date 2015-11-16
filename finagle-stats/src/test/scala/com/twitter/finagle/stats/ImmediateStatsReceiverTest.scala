package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ImmediateStatsReceiverTest extends FunSuite with BeforeAndAfter {

  private[this] var registry: Metrics = _

  before {
    registry = Metrics.createDetached()
  }

  private[this] def metrics(name: String): Option[Long] = {
    val sample = registry.sample()
    if (!sample.containsKey(name))
      None
    else {
      val x = sample.get(name)
      Some(x.longValue())
    }
  }

  test("ImmediateStatsReceiver report increment immediately") {
    val receiver = new ImmediateMetricsStatsReceiver(registry)

    assert(metrics("counter") == None)
    val c = receiver.counter("counter")
    c.incr()
    assert(metrics("counter") == Some(1))

    var x = 0
    assert(metrics("gauge") == None)
    val gauge = receiver.addGauge("gauge") { x }
    assert(metrics("gauge") == Some(0))
    x = 10
    assert(metrics("gauge") == Some(10))

    assert(metrics("stat") == None)
    val stat = receiver.stat("stat")
    (1 to 100) foreach { stat.add(_) }
    assert(metrics("stat.count") == Some(100))
    assert(metrics("stat.p99") == Some(99))
    assert(metrics("stat.min") == Some(1))
    assert(metrics("stat.max") == Some(100))
  }
}
