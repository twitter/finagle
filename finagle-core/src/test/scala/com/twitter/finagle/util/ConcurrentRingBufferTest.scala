package com.twitter.finagle.util

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class ConcurrentRingBufferTest extends FunSuite {
  test("ConcurrentRingBuffer should fetch entries in order") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    for (i <- 0 until N)
      assert(b.tryPut(i))

    for (i <- 0 until N)
      assert(b.tryGet() === Some(i))
  }

  test("ConcurrentRingBuffer should not overwrite entries") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    for (i <- 0 until N)
      assert(b.tryPut(i))
    assert(!b.tryPut(0))
  }

  test("ConcurrentRingBuffer should interleave puts and gets") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    for (i <- 0 until N * 100) {
      assert(b.tryPut(i))
      assert(b.tryGet() === Some(i))
    }
  }

  test("ConcurrentRingBuffer should make slots available for writing immediately") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    for (i <- 0 until N)
      assert(b.tryPut(i))

    for (i <- 0 until N) {
      assert(b.tryGet() === Some(i))
      b.tryPut(i * 2)
    }

    for (i <- 0 until N)
      assert(b.tryGet() === Some(i * 2))
  }
}
