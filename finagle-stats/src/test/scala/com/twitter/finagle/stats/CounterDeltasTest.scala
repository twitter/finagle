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
      cd.deltas(metrics.sampleCounters())("counter").intValue()

    // starting empty
    assert(cd.deltas(metrics.sampleCounters()).isEmpty)

    val counter = sr.counter("counter")
    assert(counterDelta === 0)

    // now, collect some data, and verify we are comparing against 0
    // since we haven't called update yet
    counter.incr(3)
    assert(counterDelta === 3)
    counter.incr(3)
    assert(counterDelta === 6)

    // after `update` we should be comparing against the current value, 6
    cd.update(metrics.sampleCounters())
    assert(counterDelta === 0)
    counter.incr(1)
    assert(counterDelta === 1)
  }

}
