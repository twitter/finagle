package com.twitter.finagle.stats

import org.scalatest.funsuite.AnyFunSuite

class CounterDeltasTest extends AnyFunSuite {

  test("deltas are computed based on last call to update") {
    val metrics = new Metrics()
    val sr = new MetricsStatsReceiver(metrics)

    val cd = new CounterDeltas()

    def counterDelta: Int =
      cd.deltas.find(_.hierarchicalName == "counter").get.value.toInt

    def assertNoDelta(): Unit =
      assert(!cd.deltas.exists(_.hierarchicalName == "counter"))

    // starting empty
    assert(cd.deltas.isEmpty)

    val counter = sr.counter("counter")
    assertNoDelta()

    // now, collect some data, and verify we have no deltas
    // since we have not yet called `update`
    counter.incr(3)
    assertNoDelta()
    counter.incr(3)
    assertNoDelta()

    // after `update` we should be comparing against the current value, 6
    cd.update(metrics.counters)
    assert(counterDelta == 6)
    // this increment will not be visible until the next `update`,
    // so the delta remains stable at 6
    counter.incr(1)
    assert(counterDelta == 6)
    // this `update` should now pick up that earlier increment
    cd.update(metrics.counters)
    assert(counterDelta == 1)
  }
}
