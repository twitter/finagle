package com.twitter.finagle.util

import java.util.concurrent.atomic.AtomicInteger
import collection.mutable.ArrayBuffer

class FutureLatch(initialCount: Int = 0) {
  require(initialCount >= 0)
  private[this] var count = initialCount
  private[this] var waiters = new ArrayBuffer[() => Unit]

  def await(f: => Unit) = synchronized {
    if (count == 0)
      f
    else
      waiters += { () => f }
  }

  def incr() = synchronized { count += 1 }

  def decr() = synchronized {
    count -= 1
    if (count == 0) {
      waiters foreach { _() }
      waiters.clear()
    }
  }
}










