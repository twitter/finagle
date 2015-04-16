package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.common.quantity
import com.twitter.common.util.Clock
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MetricsBucketedHistogramTest extends FunSuite {

  class FakeClock extends Clock {
    private[this] val nowInNanos = new AtomicLong(System.currentTimeMillis())

    def advance(delta: Duration): Unit =
      nowInNanos.addAndGet(delta.inNanoseconds)

    override def nowMillis(): Long =
      TimeUnit.NANOSECONDS.toMillis(nowNanos())

    override def nowNanos(): Long =
      nowInNanos.get()

    override def waitFor(millis: Long): Unit =
      advance(Duration.fromMilliseconds(millis))
  }

  test("basics") {
    val clock = new FakeClock

    val ps = Array[Double](0.5, 0.9)
    val h = new MetricsBucketedHistogram(
      name = "h",
      window = quantity.Amount.of(1L, quantity.Time.MINUTES),
      slices = 2,
      percentiles = ps,
      clock = clock)

    // add some data
    1L.to(100L).foreach(h.add)

    // since we have not rolled to the next window, we should not see that data
    val snap0 = h.snapshot()
    assert(snap0.min() == 0)
    assert(snap0.max() == 0)
    assert(snap0.count() == 0)
    assert(snap0.sum() == 0)
    assert(snap0.avg() == 0)
    assert(snap0.percentiles().map(_.getValue) === Array(0, 0))

    // roll to the next window
    clock.advance(31.seconds)

    // add a data point to that next window (it will not be visible in the snapshot)
    h.add(1000)
    val snap1 = h.snapshot()
    assert(snap1.min() == 1)
    assert(snap1.max() == 100)
    assert(snap1.count() == 100)
    assert(snap1.sum() == 1.to(100).sum)
    assert(snap1.avg() == 50.5d)
    assert(snap1.percentiles().map(_.getValue) === Array(50, 90))

    // fill out this 2nd window and roll the window
    101L.to(999L).foreach(h.add)
    clock.advance(31.seconds)
    val snap2 = h.snapshot()
    assert(snap2.min() == 1)
    assert(snap2.max() == 1000)
    assert(snap2.count() == 1000)
    assert(snap2.sum() == 1.to(1000).sum)
    assert(snap2.avg() == 500.5d)
    assert(snap2.percentiles().map(_.getValue) === Array(500, 899))

    // roll to the next window, which should evict that 1st window
    clock.advance(31.seconds)
    val snap3 = h.snapshot()
    assert(snap3.min() == 101L)
    assert(snap3.max() == 1000L)
    assert(snap3.count() == 900)
    assert(snap3.sum() == 101.to(1000).sum)
    assert(snap3.avg() == 550.5d)
    assert(snap3.percentiles().map(_.getValue) === Array(552, 908))

    // then clearing should wipe it all.
    1L.to(10L).foreach(h.add)
    h.clear()
    clock.advance(31.seconds)
    val snap4 = h.snapshot()
    assert(snap4.min() == 0)
    assert(snap4.max() == 0)
    assert(snap4.count() == 0)
    assert(snap4.sum() == 0)
    assert(snap4.avg() == 0)
    assert(snap4.percentiles().map(_.getValue) === Array(0, 0))
  }

}
