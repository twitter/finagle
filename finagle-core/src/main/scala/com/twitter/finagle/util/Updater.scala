package com.twitter.finagle.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ArrayBuffer

/**
 * An Updater processes updates in sequence. At most one
 * update is processed at time; pending updates may be collapsed.
 *
 * When an update is in progress, new updates are enqueued to
 * be handled by the thread currently processing updates; the
 * control is returned to the caller thread immediately. Thus
 * the thread issuing the first update will process all updates
 * until the queue of updates has been drained.
 *
 * Fairness is not guaranteed. Provided a sufficiently high rate of
 * updates, the hapless handler thread may be stuck with the job
 * indefinitely.
 */
private[finagle] trait Updater[T] extends (T => Unit) {
  private[this] val n = new AtomicInteger(0)
  private[this] val q = new ConcurrentLinkedQueue[T]

  /**
   * Preprocess a nonempty batch of updates. This allows the updater
   * to collapse, expand, or otherwise manipulate updates before they
   * are handled.
   */
  protected def preprocess(elems: Seq[T]): Seq[T]

  /**
   * Handle a single update.
   */
  protected def handle(elem: T): Unit

  def apply(t: T): Unit = {
    q.offer(t)
    if (n.getAndIncrement() > 0)
      return

    do {
      val elems = new ArrayBuffer[T](1 + n.get)
      while (n.get > 1) {
        n.decrementAndGet()
        elems += q.poll()
      }

      elems += q.poll()
      // toSeq needed for 2.13 compat
      preprocess(elems.toSeq).foreach(handle)

    } while (n.decrementAndGet() > 0)
  }
}
