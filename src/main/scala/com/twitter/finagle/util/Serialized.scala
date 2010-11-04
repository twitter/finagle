package com.twitter.finagle.util

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

trait Serialized {
  private val nwaiters = new AtomicInteger(0)
  private val executionQueue = new LinkedBlockingQueue[() => Unit]

  def serialized[T](f: T => Unit): T => Unit = { x => serialized { f(x) } }
  def serialized(f: => Any) {
    executionQueue offer { () => f }

    if (nwaiters.getAndIncrement() == 0) {
      do {
        executionQueue.poll()()
      } while (nwaiters.decrementAndGet() > 0)
    }
  }
}
