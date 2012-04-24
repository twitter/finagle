package com.twitter.finagle.util

import org.specs.SpecificationWithJUnit

class AsyncLatchSpec extends SpecificationWithJUnit {
  "when count=0, AsyncLatch" should {
    "execute waiters immediately" in {
      val latch = new AsyncLatch(0)
      var didCall = false
      latch await { didCall = true }
      didCall must beTrue
    }
  }

  "when count>0, AsyncLatch" should {
    "execute waiters when count has reached 0" in {
      val latch = new AsyncLatch(1)
      var didCall = false
      latch await { didCall = true }
      didCall must beFalse
      latch.decr()
      didCall must beTrue
    }

    "not re-execute waiters when the count increases again" in {
      val latch = new AsyncLatch(1)
      var count0 = 0
      var count1 = 0
      latch await { count0 += 1 }
      count0 must be_==(0)
      latch.decr()
      count0 must be_==(1)
      count1 must be_==(0)

      latch.incr()
      latch await { count1 += 1 }
      count0 must be_==(1)
      count1 must be_==(0)

      latch.decr()
      count0 must be_==(1)
      count1 must be_==(1)
    }

    "return count on increment" in {
      val latch = new AsyncLatch(0)
      latch.incr() mustEqual 1
    }

    "return count on decrement" in {
      val latch = new AsyncLatch(1)
      latch.decr() mustEqual 0
    }
  }
}
