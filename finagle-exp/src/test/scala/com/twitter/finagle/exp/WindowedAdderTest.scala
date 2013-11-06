package com.twitter.finagle.exp

import com.twitter.util.{Duration, Time}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfterEach}
import org.scalatest.junit.JUnitRunner
import com.twitter.conversions.time._

@RunWith(classOf[JUnitRunner])
class WindowedAdderTest extends FunSuite {
  private trait AdderHelper {
    val adder = new WindowedAdder(3.seconds, 3)
  }

  test("sums things up when time stands still") {
    new AdderHelper {
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
      new AdderHelper {
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
  }

  test("maintains a sliding window when slices are skipped") {
    Time.withCurrentTimeFrozen { tc =>
      new AdderHelper {
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
  }
}
