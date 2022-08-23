package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.util.Local
import com.twitter.finagle.exp.fiber_scheduler.fiber.Fiber
import com.twitter.finagle.exp.fiber_scheduler.fiber.ForkedFiber
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

/**
 * Workers use a custom thread class to save thread local values
 * without having to rely on `ThreadLocal` lookups.
 *
 * IMPORTANT: The fields in this class *MUST* only be read and
 * modified by the thread that is running the worker in `currentWorker`.
 * The only exception is `currentFiber`, which can read by other
 * threads but modified only by the worker.
 */
private final class WorkerThread(group: ThreadGroup, runnable: Runnable, name: String)
    extends Thread(group, runnable, name) {

  // The worker using this thread
  private[fiber_scheduler] var currentWorker: Worker = null

  // Stop handler re-used by `ForkedFiber` to avoid allocations
  private[fiber_scheduler] val stopHandler = new ForkedFiber.StopHandler()

  // The fiber being executed by the worker.
  // It's volatile because `Worker.isBlocked`
  // can be called from other threads and can't
  // to have a stale value.
  @volatile private[fiber_scheduler] var currentFiber: ForkedFiber = null

  // A thread local getter for the fiber Local.
  // The getter can't be initialized inline in
  // the constructor because another thread
  // creates the Thread object and the getter
  // must be created by the thread that is
  // going to use it.
  private[fiber_scheduler] var fiberLocalGetter: Local.ThreadLocalGetter[Fiber] = null

  override def run() = {
    // initialize the thread local getter
    fiberLocalGetter = Fiber.futureLocal.threadLocalGetter()
    super.run()
  }
}

private final object WorkerThread {

  def apply(): WorkerThread = {
    Thread.currentThread() match {
      case t: WorkerThread => t
      case _ => null
    }
  }

  val factory = new ThreadFactory {
    val name = "fiber/pool"
    val group: ThreadGroup = new ThreadGroup(Thread.currentThread().getThreadGroup(), name)
    val threadNumber: AtomicInteger = new AtomicInteger(1)

    def newThread(r: Runnable): WorkerThread = {
      val thread = new WorkerThread(group, r, name + "-" + threadNumber.getAndIncrement())
      thread.setDaemon(true)
      if (thread.getPriority != Thread.NORM_PRIORITY) {
        thread.setPriority(Thread.NORM_PRIORITY)
      }
      thread
    }
  }
}
