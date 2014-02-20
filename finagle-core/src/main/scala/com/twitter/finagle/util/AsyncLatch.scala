package com.twitter.finagle.util

import collection.mutable.ArrayBuffer

/**
 * A construct providing latched, asynchronous execution of side-effecting
 * functions. [[com.twitter.finagle.util.AsyncLatch#await]] does not block, but
 * the execution of functions passed to it may be delayed. That is, because the
 * latch guarantees serial, non-concurrent execution, functions passed to
 * `await` may have to wait until both of the following conditions are met:
 *
 * 1. No other functions are being executed
 * 2. The latch's count is zero
 *
 * Thus, calling [[com.twitter.finagle.util.AsyncLatch#incr]] will cause
 * subsequent functions passed to [[com.twitter.finagle.util.AsyncLatch#await]]
 * to be delayed until the count is decremented to zero. Once the count is zero,
 * functions are executed in the order that they are provided to `await`.
 */
class AsyncLatch(initialCount: Int = 0) {
  require(initialCount >= 0)
  @volatile private[this] var count = initialCount
  private[this] var waiters = new ArrayBuffer[() => Unit]

  /**
   * Execute the given computation when the count of this latch has
   * reached zero.
   */
  def await(f: => Unit): Unit = synchronized {
    if (count == 0)
      f
    else
      waiters += { () => f }
  }

  /**
   * Increment the latch. Computations passed to `await` will not be executed
   * until the latch's count returns to zero.
   *
   * @return the latch's count after being incremented
   */
  def incr(): Int = synchronized { count += 1; count }

  /**
   * Decrement the latch. If the latch's count reaches zero, awaiting
   * computations are executed serially.
   *
   * @return the latch's count after being decremented
   */
  def decr(): Int = {
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

  /**
   * @return the latch's current count
   */
  def getCount: Int = count
}
