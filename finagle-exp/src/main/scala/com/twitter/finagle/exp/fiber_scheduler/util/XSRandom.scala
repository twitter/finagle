package com.twitter.finagle.exp.fiber_scheduler.util

import java.util.Random

/**
 * A thread-safe and low-overhead random number generator using XorShift.
 * The entropy of the implementation is low but it produces a good distribution
 * of values, which is enough for some use cases.
 */
private[fiber_scheduler] final object XSRandom extends Random {
  private[this] var seed: Long = System.nanoTime()
  override def next(nbits: Int): Int = {
    var x = seed + Thread.currentThread().getId
    x ^= (x << 21)
    x ^= (x >>> 35)
    x ^= (x << 4)
    seed = x
    x &= ((1L << nbits) - 1)
    x.asInstanceOf[Int]
  }
}
