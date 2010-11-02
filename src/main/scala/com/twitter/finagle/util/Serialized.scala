package com.twitter.finagle.util

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

class Serialized {
  private val nwaiters = new AtomicInteger(0)
  private val executionQueue = new LinkedBlockingQueue[Function0[Unit]]

  def apply[T](f: T => Unit): T => Unit = { x => this { f(x) } }
  def apply(f: => Unit) {
    executionQueue offer { () => f }

    if (nwaiters.getAndIncrement() == 0) {
      do {
        executionQueue.poll()()
      } while (nwaiters.decrementAndGet() > 0)
    }
  }
}
