package com.twitter.finagle.util

import com.twitter.util.{Time, Stopwatch}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.conversions.time._

@RunWith(classOf[JUnitRunner])
class TokenBucketTest extends FunSuite {
  test("a leaky bucket is leaky") {
    Time.withCurrentTimeFrozen { tc =>
      val b = TokenBucket.newLeakyBucket(3.seconds, 0, Stopwatch.timeMillis)
      b.put(100)
      assert(b.tryGet(1))

      tc.advance(3.seconds)
      assert(!b.tryGet(1))
    }
  }

  test("tryGet fails when empty") {
    Time.withCurrentTimeFrozen { tc =>
      val b = TokenBucket.newLeakyBucket(3.seconds, 0, Stopwatch.timeMillis)
      b.put(100)
      assert(b.tryGet(50))
      assert(b.tryGet(49))
      assert(b.tryGet(1))
      assert(!b.tryGet(1))
      assert(!b.tryGet(50))
      b.put(1)
      assert(!b.tryGet(2))
      assert(b.tryGet(1))
      assert(!b.tryGet(1))
    }
  }

  test("provisions reserves") {
    Time.withCurrentTimeFrozen { tc =>
      val b = TokenBucket.newLeakyBucket(3.seconds, 100, Stopwatch.timeMillis)

      // start at 0, though with 100 in reserve
      assert(b.tryGet(50)) // -50 + 100 = 0
      assert(b.tryGet(50)) // -100 + 100 = 0
      assert(!b.tryGet(1)) // nope, at 0
      b.put(1) // now at -99 + 100 = 1
      assert(b.tryGet(1)) // back to 0

      tc.advance(1.second)
      // This is what you get for eating
      // all of your candy right away.
      assert(!b.tryGet(1)) // still at -100 + 100 = 0

      tc.advance(1.second)
      assert(!b.tryGet(1)) // still at -100 + 100 = 0

      tc.advance(1.second)
      assert(!b.tryGet(1)) // still at -100 + 100 = 0

      tc.advance(1.second)
      assert(b.tryGet(50)) // the -100 expired, so -50 + 100 = 50

      tc.advance(3.seconds) // the -50 expired, so -100 + 100 = 0
      assert(b.tryGet(100))
      assert(!b.tryGet(1))
    }
  }
}
