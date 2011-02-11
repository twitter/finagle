package com.twitter.finagle.util

/** 
 * An AsyncSemaphore is a traditional semaphore but with asynchronous
 * execution. That is: grabbing a permit defers execution of the
 * passed-in computation until the permit is available.
 */ 

import collection.mutable.Queue

class AsyncSemaphore(initialPermits: Int = 0) {
  private[this] var waiters = new Queue[() => Unit]
  private[this] var availablePermits = initialPermits

  def numWaiters = synchronized { waiters.size }
  def numPermitsAvailable = availablePermits

  def acquire(f: => Unit) = {
    val runNow = synchronized {
      if (availablePermits > 0) {
        availablePermits -= 1
        true
      } else {
        waiters enqueue { () => f }
        false
      }
    }

    if (runNow) f
  }

  def release() = {
    val run = synchronized {
      availablePermits += 1
      if (availablePermits > 0 && !waiters.isEmpty) {
        availablePermits -= 1
        Some(waiters.dequeue())
      } else {
        None
      }
    }

    run foreach { _() }
  }
}
