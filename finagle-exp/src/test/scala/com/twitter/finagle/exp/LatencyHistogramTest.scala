package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.util.{Duration, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class LatencyHistogramTest extends FunSuite {
  val range = 10 * 1000 // 10 seconds

  def testRandom(rng: Random, N: Int) {
    val histo = new LatencyHistogram(range, Duration.Top.inMilliseconds, WindowedAdder.timeMs)
    val input = Array.fill(N) {
      math.abs(rng.nextInt) % range
    }
    for (d <- input)
      histo.add(d)
    val sorted = input.sorted
    for (q <- 0 until 100)
      assert(histo.quantile(q) === sorted(q*N/100))
  }

  val tests = Seq(
    (130827L, 300),
    (130655L, 200),
    (127290L, 800),
    (128163L, 123)
  )

  for ((seed, n) <- tests)
    test("random: %d %d".format(seed, n)) {
      testRandom(new Random(seed), n)
    }

  // Since the sliding window internally is sliced, we shouldn't make
  // too many assumptions about which updates are dropped, only that
  // they are dropped by the windowed time.
  //
  // We can also assume that all updates inside of a window should be
  // reflected, but no others.
  //
  // So we test these two properties instead of a sliding window
  // directly.
  test("maintains sliding window by time") {
    Time.withCurrentTimeFrozen { tc =>
    val histo = new LatencyHistogram(range=40,
      history=4000, WindowedAdder.timeMs)
      for (_ <- 0 until 100) histo.add(30)
      tc.advance(1.second)

      for (_ <- 0 until 100) histo.add(10)
      assert(histo.quantile(99) == 30)

      tc.advance(1.second)
      assert(histo.quantile(99) == 30)
      tc.advance(1.second)
      assert(histo.quantile(99) == 30)
      tc.advance(2.seconds)
      assert(histo.quantile(99) == 10)
    }
  }

  test("handles durations longer than range by not exploding") {
    val histo = new LatencyHistogram(range=40,
      history=4 * 1000, WindowedAdder.timeMs)
    histo.add(40)
  }
}
