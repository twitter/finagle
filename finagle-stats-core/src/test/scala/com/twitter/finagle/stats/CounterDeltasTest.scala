package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.UnlatchedCounter
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class CounterDeltasTest extends AnyFunSuite with OneInstancePerTest {

  private[this] val metrics = Metrics.createDetached()
  private[this] val sr = new MetricsStatsReceiver(metrics)
  private[this] val cd = new CounterDeltas()

  private[this] def counterDelta(hierarchicalName: String): Int =
    cd.deltas.find(_.hierarchicalName == hierarchicalName).get.value.toInt

  test("deltas are computed based on last call to update") {

    def getDelta: Int = counterDelta("counter")

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
    assert(getDelta == 6)
    // this increment will not be visible until the next `update`,
    // so the delta remains stable at 6
    counter.incr(1)
    assert(getDelta == 6)
    // this `update` should now pick up that earlier increment
    cd.update(metrics.counters)
    assert(getDelta == 1)
  }

  test("UnlatchedCounter type is respected") {
    val unlatched =
      sr.counter(MetricBuilder(name = Seq("unlatched_counter"), metricType = UnlatchedCounter))

    val latched =
      sr.counter(MetricBuilder(name = Seq("latched_counter"), metricType = CounterType))

    def getUnlatched: Int = counterDelta("unlatched_counter")

    def getLatched: Int = counterDelta("latched_counter")

    cd.update(metrics.counters)

    assert(getUnlatched == 0)
    assert(getLatched == 0)

    unlatched.incr()
    latched.incr()
    cd.update(metrics.counters)

    // First entry should be the same for both since they were both at 0 to start.
    assert(getUnlatched == 1)
    assert(getLatched == 1)

    // In this case we should see that the unlatched counter didn't display 'latched' behavior
    unlatched.incr()
    latched.incr()
    cd.update(metrics.counters)

    assert(getUnlatched == 2)
    assert(getLatched == 1)

    // Finally, update again with no increments
    cd.update(metrics.counters)
    assert(getUnlatched == 2)
    assert(getLatched == 0)
  }
}
