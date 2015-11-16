package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.util.{Duration, Time, Stopwatch}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class LatencyHistogramTest extends FunSuite
  with Matchers
{
  val range = 10 * 1000 // 10 seconds

  def testRandom(rng: Random, N: Int, err: Double): Unit = {
    val histo = new LatencyHistogram(
      range, err, Duration.Top.inMilliseconds, LatencyHistogram.DefaultSlices, Stopwatch.timeMillis)
    val input = Array.fill(N) {
      (rng.nextDouble() * range).toLong
    }
    for (d <- input)
      histo.add(d)

    val epsilon = if (err == 0.0) 0 else range * err / 2
    val sorted = input.sorted
    for (q <- 0 until 100) {
      withClue(s"quantile $q: ") {
        val actual = histo.quantile(q)
        val ideal = sorted(q * N / 100)
        if (epsilon == 0)
          assert(actual == ideal)
        else
          actual.toDouble should be(ideal.toDouble +- epsilon)
      }
    }
  }

  val tests = Seq(
    (130827L, 300),
    (130655L, 200),
    (127290L, 800),
    (128163L, 123)
  )

  Seq(0.0, 0.1, 0.01).foreach { err =>
    for ((seed, n) <- tests)
      test(s"random: seed=$seed num=$n error=$err") {
        testRandom(new Random(seed), n, err)
      }
  }

  test("constructor checks inputs") {
    intercept[IllegalArgumentException] {
      new LatencyHistogram(-1L, 0.0, 1L, LatencyHistogram.DefaultSlices, Stopwatch.systemMillis)
    }
    intercept[IllegalArgumentException] {
      new LatencyHistogram(1L, -0.1, 1L, LatencyHistogram.DefaultSlices, Stopwatch.systemMillis)
    }
    intercept[IllegalArgumentException] {
      new LatencyHistogram(1L, 1.1, 1L, LatencyHistogram.DefaultSlices, Stopwatch.systemMillis)
    }
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
      val histo = new LatencyHistogram(
        clipDuration = 40,
        error = 0.0,
        history = 4000,
        slices = LatencyHistogram.DefaultSlices,
        now = Stopwatch.timeMillis)
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
    val histo = new LatencyHistogram(
      clipDuration = 40,
      error = 0.0,
      history = 4 * 1000,
      slices = LatencyHistogram.DefaultSlices,
      now = Stopwatch.timeMillis)
    histo.add(40)
  }
}
