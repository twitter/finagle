package com.twitter.finagle.util

import org.scalatest.funsuite.AnyFunSuite

class ConcurrentRingBufferTest extends AnyFunSuite {
  test("ConcurrentRingBuffer should fetch entries in order") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    for (i <- 0 until N)
      assert(b.tryPut(i))

    for (i <- 0 until N)
      assert(b.tryGet() == Some(i))
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
      assert(b.tryGet() == Some(i))
    }
  }

  test("ConcurrentRingBuffer should make slots available for writing immediately") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    for (i <- 0 until N)
      assert(b.tryPut(i))

    for (i <- 0 until N) {
      assert(b.tryGet() == Some(i))
      b.tryPut(i * 2)
    }

    for (i <- 0 until N)
      assert(b.tryGet() == Some(i * 2))
  }

  test("ConcurrentRingBuffer should return the size correctly") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    for (i <- 0 until N / 2) assert(b.tryPut(i))

    assert(b.size == 64)

    b.tryGet()

    assert(b.size == 63)
  }

  test("ConcurrentRingBuffer should allow peeking") {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    assert(b.tryPeek == None)

    for (i <- 0 until N / 2) {
      assert(b.tryPut(i))
      assert(b.tryPeek == Some(0))
    }

    assert(b.size == 64)

    for (i <- 0 until N / 2) {
      assert(b.tryPeek == Some(i))
      assert(b.tryGet() == Some(i))
    }

    assert(b.tryGet() == None)
    assert(b.size == 0)

    for (i <- N / 2 until N) b.tryGet() // fully drain

    assert(b.tryPeek == None)
  }
}
