package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CounterDeltasTest extends FunSuite {

  test("deltas are computed based on last call to update") {
    val metrics = Metrics.createDetached()
    val sr = new MetricsStatsReceiver(metrics)

    val cd = new CounterDeltas()

    def counterDelta: Int =
      cd.deltas("counter").intValue()

    def assertNoDelta(): Unit =
      assert(!cd.deltas.contains("counter"))

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
    cd.update(metrics.sampleCounters())
    assert(counterDelta == 6)
    // this increment will not be visible until the next `update`,
    // so the delta remains stable at 6
    counter.incr(1)
    assert(counterDelta == 6)
    // this `update` should now pick up that earlier increment
    cd.update(metrics.sampleCounters())
    assert(counterDelta == 1)
  }

}
