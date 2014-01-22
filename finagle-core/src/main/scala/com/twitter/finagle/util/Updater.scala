package com.twitter.finagle.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Assigns an integral priority to `T`-typed values. Smaller
 * priorities are higher.
 */
private[finagle] trait Prioritized[T] extends (T => Int)

/**
 * Process updates of type `T` with the function `f`. When an update
 * arrives while another is being processed, it is queued, to be
 * delivered after the current update completes. When multiple
 * updates have been enqueued, we deliver only the one with the
 * lowest priority (according to the
 * [[com.twitter.finagle.util.Prioritized Prioritized]] instance);
 * when multiple items have the same priority, the one which was
 * enqueued last is chosen.
 */
private class Updater[T: Prioritized](f: T => Unit) extends (T => Unit) {
  private[this] val n = new AtomicInteger(0)
  private[this] val q = new ConcurrentLinkedQueue[T]
  private[this] val pri = implicitly[Prioritized[T]]

  def apply(t: T) {
    q.offer(t)
    if (n.getAndIncrement() > 0)
      return

    do {
      var el = q.peek()
      while (n.get > 1) {
        n.decrementAndGet()
        val el1 = q.poll()
        if (pri(el1) <= pri(el))
          el = el1
      }

      val el1 = q.poll()
      val min = if (pri(el1) <= pri(el)) el1 else el
      f(min)
    } while (n.decrementAndGet() > 0)
  }
}

private[finagle] object Updater {
  def apply[T: Prioritized](f: T => Unit): (T => Unit) = new Updater(f)
}
