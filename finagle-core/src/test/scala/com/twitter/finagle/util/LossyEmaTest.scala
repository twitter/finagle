package com.twitter.finagle.util

import com.twitter.conversions.DurationOps._
import com.twitter.util.{Stopwatch, Time}
import org.scalatest.Matchers.convertNumericToPlusOrMinusWrapper
import org.scalatest.funsuite.AnyFunSuite

class LossyEmaTest extends AnyFunSuite {

  private class TestNow(private var now: Long) extends (() => Long) {
    def apply(): Long = now

    def update(newNow: Long): Unit = now = newNow
  }

  private class Updater(window: Int, initialNow: Long, initialValue: Double) {
    private val now: TestNow = new TestNow(initialNow)
    val ema = new LossyEma(window, now, initialValue)

    def update(newNow: Int, newValue: Int): Double = {
      now.update(newNow)
      ema.update(newValue)
    }
  }

  test("windowMicros must be positive") {
    intercept[IllegalArgumentException] {
      new LossyEma(0L, Stopwatch.systemNanos, 100.0)
    }
  }

  test("update") {
    val a, b = new Updater(1000, 10L, 10)
    assert(a.ema.last == 10)
    assert(a.update(20, 10) == 10)
    assert(a.update(30, 10) == 10)

    assert(b.ema.last == 10)
    assert(b.update(30, 10) == 10)

    assert(a.update(40, 5) == b.update(40, 5))

    assert(a.update(50, 5) > a.update(60, 5))
  }

  test("update over durations greater than window") {
    Time.withCurrentTimeFrozen { tc =>
      val epsilon = 0.000000000000025
      val value = 100.0
      val ema = new LossyEma(300.millis.inMillis, Stopwatch.timeMillis, value)

      tc.advance(100.millis)
      assert(ema.update(0.0) === 71.65313105737893 +- epsilon)

      tc.advance(100.millis)
      assert(ema.update(0.0) === 51.34171190325921 +- epsilon)

      tc.advance(100.millis)
      assert(ema.update(0.0) === 36.787944117144235 +- epsilon)

      tc.advance(100.millis)
      assert(ema.update(0.0) === 26.35971381157268 +- epsilon)

      tc.advance(100.millis)
      assert(ema.update(0.0) === 18.887560283756187 +- epsilon)
    }
  }

  test("update when time does not advance") {
    Time.withCurrentTimeFrozen { tc =>
      val value = 500.0
      val ema = new LossyEma(5000, Stopwatch.timeMicros, value)

      assert(ema.last == value)

      // updates will be ignored as time doesn't change.
      assert(ema.update(5e10) == value)
      assert(ema.update(-3333.3) == value)
      assert(ema.update(0.0) == value)

      // walk time backwards. still no changes.
      tc.advance(-1.microsecond)
      assert(ema.update(5e10) == value)
      assert(ema.update(-3333.3) == value)
      assert(ema.update(0.0) == value)

      // ok, advance it
      tc.advance(400.microseconds)
      assert(ema.update(5e10) != value)
    }
  }

  test("reset") {
    val initialVal = 3.0
    val ema = new LossyEma(5000, Stopwatch.systemNanos, initialVal)
    assert(ema.update(1000.0) >= 3.0)

    ema.reset()

    assert(ema.last == initialVal)
  }
}
