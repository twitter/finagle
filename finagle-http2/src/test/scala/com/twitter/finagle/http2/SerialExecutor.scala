package com.twitter.finagle.http2

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executor
import scala.collection.mutable.ArrayBuffer

/**
 * Ensure serial execution of runnables without synchronization blocks to avoid
 * deadlocks.
 * This functionality is typically provided by netty's event loop in non-testing
 * situations.
 */
class SerialExecutor extends Executor {
  private[this] val n = new AtomicInteger(0)
  private[this] val q = new ConcurrentLinkedQueue[Runnable]

  def execute(r: Runnable): Unit = {
    q.offer(r)
    if (n.getAndIncrement() > 0)
      return

    do {
      val elems = new ArrayBuffer[Runnable](1 + n.get)
      while (n.get > 1) {
        n.decrementAndGet()
        elems += q.poll()
      }

      elems += q.poll()
      elems.foreach(_.run())

    } while (n.decrementAndGet() > 0)
  }
}
