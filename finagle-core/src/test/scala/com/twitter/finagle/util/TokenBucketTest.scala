package com.twitter.finagle.util

import com.twitter.util.Time
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfterEach}
import org.scalatest.junit.JUnitRunner
import com.twitter.conversions.time._

@RunWith(classOf[JUnitRunner])
class TokenBucketTest extends FunSuite {
  test("a leaky bucket is leaky") {
    Time.withCurrentTimeFrozen { tc =>
      val b = TokenBucket.newLeakyBucket(3.seconds, 0, WindowedAdder.timeMs)
      b.put(100)
      assert(b.tryGet(1))

      tc.advance(3.seconds)
      assert(!b.tryGet(1))
    }
  }

  test("tryGet fails when empty") {
    Time.withCurrentTimeFrozen { tc =>
      val b = TokenBucket.newLeakyBucket(3.seconds, 0, WindowedAdder.timeMs)
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
      val b = TokenBucket.newLeakyBucket(3.seconds, 100, WindowedAdder.timeMs)

      assert(b.tryGet(50))
      assert(b.tryGet(50))
      assert(!b.tryGet(1))
      b.put(1)
      assert(b.tryGet(1))

      tc.advance(1.second)
      // This is what you get for eating
      // all of your candy right away.
      assert(!b.tryGet(1))

      tc.advance(1.second)
      assert(!b.tryGet(1))

      tc.advance(1.second)
      assert(!b.tryGet(1))

      tc.advance(1.second)
      assert(b.tryGet(50))

      tc.advance(3.seconds)
      assert(b.tryGet(100))
      assert(!b.tryGet(1))
    }
  }
}
