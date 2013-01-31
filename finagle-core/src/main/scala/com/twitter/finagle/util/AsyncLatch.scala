package com.twitter.finagle.util

import collection.mutable.ArrayBuffer

/**
 * The AsyncLatch is an asynchronous latch.
 */
class AsyncLatch(initialCount: Int = 0) {
  require(initialCount >= 0)
  @volatile private[this] var count = initialCount
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
  def incr() = synchronized { count += 1; count }

  /**
   * Decrement the latch. If the latch value reaches 0, awaiting
   * computations are executed inline.
   */
  def decr() = {
    val pendingTasks = synchronized {
      require(count > 0)
      count -= 1
      if (count == 0) {
        val pending = waiters
        waiters = new ArrayBuffer[() => Unit]
        Left(pending)
      } else {
        Right(count)
      }
    }

    pendingTasks match {
      case Left(tasks) =>
        tasks foreach { _() }; 0
      case Right(count) =>
        count
    }
  }

  def getCount = count
}
