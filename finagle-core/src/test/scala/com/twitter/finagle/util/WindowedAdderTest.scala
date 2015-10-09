package com.twitter.finagle.util

import com.twitter.conversions.time._
import com.twitter.util.Time
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WindowedAdderTest extends FunSuite {
  private def newAdder() = WindowedAdder(3 * 1000, 3, WindowedAdder.timeMs)

  test("sums things up when time stands still") {
    Time.withCurrentTimeFrozen { tc =>
      val adder = newAdder()
      adder.add(1)
      assert(adder.sum() === 1)
      adder.add(1)
      assert(adder.sum() === 2)
      adder.add(3)
      assert(adder.sum() === 5)
    }
  }

  test("maintains a sliding window") {
    Time.withCurrentTimeFrozen { tc =>
      val adder = newAdder()
      adder.add(1)
      assert(adder.sum() === 1)
      tc.advance(1.second)
      assert(adder.sum() === 1)
      adder.add(2)
      assert(adder.sum() === 3)
      tc.advance(1.second)
      assert(adder.sum() === 3)
      tc.advance(1.second)
      assert(adder.sum() === 2)
      tc.advance(1.second)
      assert(adder.sum() === 0)
    }
  }

  test("maintains a sliding window when slices are skipped") {
    Time.withCurrentTimeFrozen { tc =>
      val adder = newAdder()
      adder.incr()
      assert(adder.sum() === 1)
      tc.advance(1.seconds)
      adder.add(2)
      assert(adder.sum() === 3)
      tc.advance(1.second)
      adder.incr()
      assert(adder.sum() === 4)

      tc.advance(2.seconds)
      assert(adder.sum() === 1)

      tc.advance(100.seconds)
      assert(adder.sum() === 0)

      adder.add(100)
      tc.advance(1.second)
      assert(adder.sum() === 100)
      adder.add(100)
      tc.advance(1.second)
      adder.add(100)
      assert(adder.sum() === 300)
      tc.advance(100.seconds)
      assert(adder.sum() === 0)
    }
  }

  test("maintains negative sums") {
    Time.withCurrentTimeFrozen { tc =>
      val adder = newAdder()
      // net: 2
      adder.add(-2)
      assert(adder.sum() == -2)
      adder.add(4)
      assert(adder.sum() == 2)

      // net: -4
      tc.advance(1.second)
      adder.add(-2)
      assert(adder.sum() == 0)
      adder.add(-2)
      assert(adder.sum() == -2)

      // net: -2
      tc.advance(1.second)
      adder.add(-2)
      assert(adder.sum() == -4)

      tc.advance(1.second)
      assert(adder.sum() == -6)

      tc.advance(1.second)
      assert(adder.sum() == -2)

      tc.advance(1.second)
      assert(adder.sum() == 0)

      tc.advance(100.seconds)
      assert(adder.sum() == 0)
    }
  }
}
