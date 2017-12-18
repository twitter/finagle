package com.twitter.finagle.util

import com.twitter.conversions.time._
import com.twitter.util.{MockTimer, Time}
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

class WindowedPercentileHistogramTest extends FunSuite with Eventually with IntegrationPatience {

  test("Throws IllegalArgumentException when retrieving a percentile < 0") {
    val wp = new WindowedPercentileHistogram(10, 10.seconds, new MockTimer)
    wp.percentile(0)
    intercept[IllegalArgumentException] {
      wp.percentile(-0.5)
    }
  }

  test("Throws IllegalArgumentException when retrieving a percentile > 100") {
    val wp = new WindowedPercentileHistogram(10, 10.seconds, new MockTimer)
    wp.percentile(100)
    intercept[IllegalArgumentException] {
      wp.percentile(100.5)
    }
  }

  test("Percentile is initially 0") {
    val wp = new WindowedPercentileHistogram(10, 10.seconds, new MockTimer)
    assert(wp.percentile(50.0) == 0)
    wp.close()
  }

  test("Gets fractional percentiles to two decimal places accurately") {
    val timer = new MockTimer
    0.until(9999).foreach { i: Int =>
      Time.withCurrentTimeFrozen { tc =>
        val wp = new WindowedPercentileHistogram(10, 10.seconds, timer)
        val percentile = i.toDouble / 100
        val initial = (10000 - i - 1)
        0.until(initial).foreach(_ => wp.add(10))
        tc.advance(10.seconds)
        timer.tick()
        assert(wp.percentile(percentile) == 10)
        0.until(10000 - initial).foreach(_ => wp.add(5))
        tc.advance(10.seconds)
        timer.tick()
        assert(wp.percentile(percentile) == 5)
        wp.close()
      }
    }
  }

  test("Buckets are flushed each `bucketSize`") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(10, 10.seconds, timer)
      wp.add(100)
      assert(wp.percentile(0.0) == 0)
      tc.advance(10.seconds)
      timer.tick()
      eventually {
        assert(wp.percentile(0.0) == 100)
        assert(wp.percentile(50.0) == 100)
        assert(wp.percentile(100.0) == 100)
      }
      wp.close()
    }
  }

  test("Sliding window") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(
        3,
        10.seconds,
        timer)

      wp.add(1)
      wp.add(2)
      wp.add(3)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(wp.percentile(50.0) == 2) // median of (1, 2, 3)
      }

      wp.add(4)
      wp.add(5)
      wp.add(6)
      wp.add(7)

      tc.advance(10.seconds)
      timer.tick()

      eventually {
        assert(wp.percentile(50.0) == 4) // median of (1, 2, 3) ∪ (4, 5, 6, 7)
      }

      wp.add(8)
      wp.add(9)
      wp.add(10)

      tc.advance(10.seconds)
      timer.tick()  // flush adds

      tc.advance(10.seconds) // first bucket (1, 2, 3) falls out of window
      timer.tick()

      eventually {
        assert(wp.percentile(0.0) == 4) // min of (4, 5, 6, 7) ∪ (8, 9, 10)
        assert(wp.percentile(50.0) == 7) // median of (4, 5, 6, 7) ∪ (8, 9, 10)
        assert(wp.percentile(100.0) == 10) // max of (4, 5, 6, 7) ∪ (8, 9, 10)
      }
      wp.close()
    }
  }

  test("Caps values at MaxHighestTrackableValue") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { tc =>
      val wp = new WindowedPercentileHistogram(
        3,
        10.seconds,
        timer)

      wp.add(WindowedPercentileHistogram.MaxHighestTrackableValue + 10)
      tc.advance(10.seconds)
      timer.tick()
      eventually {
        assert(wp.percentile(50.0) == WindowedPercentileHistogram.MaxHighestTrackableValue)
      }
    }
  }

  test("Has precision of +/- 1 at 1000") {
    val timer = new MockTimer
    1.until(WindowedPercentileHistogram.MaxHighestTrackableValue / 1000).foreach { i =>
      Time.withCurrentTimeFrozen { tc =>
        val wp = new WindowedPercentileHistogram(
          3,
          10.seconds,
          timer)

        wp.add(1000 * i)
        assert(wp.percentile(0.0) == 0)
        tc.advance(10.seconds)
        timer.tick()
        eventually {
          assert(
            wp.percentile(50.0) >= ((1000 * i) - i) &&
            wp.percentile(50.0) <= ((1000 * i) + i))
        }
      }
    }
  }
}
