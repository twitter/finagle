package com.twitter.finagle.exp.fiber_scheduler.util

/**
 * Utility object that returns the next power of two
 * of an integer.
 */
private[fiber_scheduler] final object NextPowerOfTwo {
  private[this] val log2 = Math.log(2)
  def apply(v: Int) = 1 << (Math.log(v - 1) / log2).asInstanceOf[Int] + 1
}
