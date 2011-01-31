package com.twitter.finagle.util

import java.util.concurrent.atomic.AtomicInteger
import collection.mutable.ArrayBuffer

/**
 * The FutureLatch is an asynchronous latch.
 */
class FutureLatch(initialCount: Int = 0) {
  require(initialCount >= 0)
  private[this] var count = initialCount
  private[this] var waiters = new ArrayBuffer[() => Unit]

  /**
   * Execute the given computation when the count of this latch has
   * reached 0.
   */
  def await(f: => Unit) = synchronized {
    if (count == 0)
      f
    else
      waiters += { () => f }
  }

  /**
   * Increment the latch.
   */
  def incr() = synchronized { count += 1 }

  /**
   * Decrement the latch. If the latch value reaches 0, awaiting
   * computations are executed inline.
   */
  def decr() = synchronized {
    require(count > 0)
    count -= 1
    if (count == 0) {
      waiters foreach { _() }
      waiters.clear()
    }
  }
}










