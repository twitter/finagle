package com.twitter.finagle.util

import org.scalatest.funsuite.AnyFunSuite

class AsyncLatchTest extends AnyFunSuite {
  test("when count=0, AsyncLatch should execute waiters immediately") {
    val latch = new AsyncLatch(0)
    var didCall = false
    latch await {
      didCall = true
    }
    assert(didCall)
  }

  test("when count>0, AsyncLatch should execute waiters when count has reached 0") {
    val latch = new AsyncLatch(1)
    var didCall = false
    latch await {
      didCall = true
    }
    assert(!didCall)
    latch.decr()
    assert(didCall)
  }

  test("when count>0, AsyncLatch should not re-execute waiters when the count increases again") {
    val latch = new AsyncLatch(1)
    var count0 = 0
    var count1 = 0
    latch await {
      count0 += 1
    }
    assert(count0 == 0)
    latch.decr()
    assert(count0 == 1)
    assert(count1 == 0)

    latch.incr()
    latch await {
      count1 += 1
    }
    assert(count0 == 1)
    assert(count1 == 0)

    latch.decr()
    assert(count0 == 1)
    assert(count1 == 1)
  }

  test("when count>0, AsyncLatch should return count on increment") {
    val latch = new AsyncLatch(0)
    assert(latch.incr() == 1)
  }

  test("when count>0, AsyncLatch should return count on decrement") {
    val latch = new AsyncLatch(1)
    assert(latch.decr() == 0)
  }
}
