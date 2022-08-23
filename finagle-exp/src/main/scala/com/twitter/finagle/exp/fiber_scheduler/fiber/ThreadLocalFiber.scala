package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.util.AvoidLoop

/**
 * The default type of fiber that is used when there isn't a fiber
 * available. It functions similarly to `LocalScheduler.Activation`.
 *
 * Since the fiber doesn't have a backing execution mechanism, even
 * if it's the parent of another fiber, the execution doesn't return back
 * to it after a nested fiber execution.
 */
private final class ThreadLocalFiber extends Fiber {

  private[this] val thread = Thread.currentThread()
  private[this] var running = false

  override protected[fiber] def hasExecutor = false

  /**
   * Adds a new task and triggers execution if necessary.
   * Only called by the owning thread so no synchronization
   * is required
   */
  override def add(r: Runnable, asOwner: Boolean): Unit = {
    super.add(r, asOwner)
    assert(thread == Thread.currentThread())
    if (!running) {
      running = true
      runTasksWithCpuTracing()
      assert(mailbox.isEmpty())
      running = false
    }
  }

  /**
   * Given a thread local fiber, all of its nested tasks
   * will be under the same trace so we can trace cpu
   * for all nested tasks within an activation at once.
   * Thread local fibers are rare and typically only
   * handle tasks by threads other than the netty
   * event loops and fiber scheduler workers, so the
   * overhead here is minimal.
   */
  private[this] def runTasksWithCpuTracing() = {
    val start = ThreadLocalFiber.cpuTimeTracing()
    val stop = start()
    runTasks()
    stop()
  }

  override def toString = AvoidLoop(
    this,
    s"ThreadLocalFiber(thread = $thread, mailbox.itemsSize = ${mailbox.itemsSize}, mailbox.inboxSize = ${mailbox.inboxSize})",
    s"ThreadLocalFiber(thread = $thread)"
  )
}

final object ThreadLocalFiber {
  private val cpuTimeTracing =
    new CpuTimeTracing("thread_local", Config.Scheduling.cpuTimeTracingPercentage)
}
