package com.twitter.finagle.util

import org.specs.SpecificationWithJUnit

class ConcurrentRingBufferSpec extends SpecificationWithJUnit {
  // ConcurrentRingBuffer's behavior is both simple and obvious; sadly the
  // complex aspects of its implementation all have to do with its
  // usage in concurrent settings. This is notoriously difficult to
  // test.
  "ConcurrentRingBuffer" should {
    val N = 128
    val b = new ConcurrentRingBuffer[Int](N)

    "fetch entries in order" in {
      for (i <- 0 until N)
        b.tryPut(i) must beTrue

      for (i <- 0 until N)
        b.tryGet() must beSome(i)
    }

    "not overwrite entries" in {
      for (i <- 0 until N)
        b.tryPut(i) must beTrue
      b.tryPut(0) must beFalse
    }

    "interleave puts and gets" in {
      for (i <- 0 until N*100) {
        b.tryPut(i) must beTrue
        b.tryGet() must beSome(i)
      }
    }

    "make slots available for writing immediately" in {
      for (i <- 0 until N)
        b.tryPut(i) must beTrue

      for (i <- 0 until N) {
        b.tryGet() must beSome(i)
        b.tryPut(i*2) must beTrue
      }

      for (i <- 0 until N)
        b.tryGet() must beSome(i*2)
    }
  }
}
