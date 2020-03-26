package com.twitter.finagle.util

private object AsyncLatch {
  private val runTask: (() => Unit) => Unit = {
    _()
  }
}

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
 * functions are executed in LIFO order, i.e., the reverse of the order in
 * which they are given to `await`.
 */
class AsyncLatch(initialCount: Int = 0) {
  import AsyncLatch._
  require(initialCount >= 0)
  @volatile private[this] var count = initialCount
  // Thread safety of `waiters` is provided by synchronization on `this`
  private[this] var waiters: List[() => Unit] = Nil

  /**
   * Execute the given computation when the count of this latch has
   * reached zero.
   */
  def await(f: => Unit): Unit = synchronized {
    if (count == 0)
      f
    else
      waiters = { () => f } :: waiters
  }

  /**
   * Increment the latch. Computations passed to `await` will not be executed
   * until the latch's count returns to zero.
   *
   * @return the latch's count after being incremented
   */
  def incr(): Int = synchronized {
    val newCount = count + 1
    count = newCount
    newCount
  }

  /**
   * Decrement the latch. If the latch's count reaches zero, awaiting
   * computations are executed serially.
   *
   * @return the latch's count after being decremented
   */
  def decr(): Int = {
    var newCount = 0

    val pendingTasks: List[() => Unit] = synchronized {
      newCount = count - 1
      require(newCount >= 0)
      count = newCount
      if (newCount == 0) {
        val pending = waiters
        waiters = Nil
        pending
      } else {
        Nil
      }
    }

    pendingTasks.foreach(runTask)
    newCount
  }

  /**
   * @return the latch's current count
   */
  def getCount: Int = count
}
