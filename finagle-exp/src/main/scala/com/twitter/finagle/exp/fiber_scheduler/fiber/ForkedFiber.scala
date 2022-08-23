package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.Worker
import com.twitter.finagle.exp.fiber_scheduler.util.AvoidLoop
import com.twitter.finagle.exp.fiber_scheduler.util.LowResClock.nowNanos
import com.twitter.finagle.exp.fiber_scheduler.util.Mailbox
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Fiber used to handle calls to the `FiberScheduler.fork` methods.
 */
private[fiber_scheduler] final class ForkedFiber(val group: Group) extends Fiber {

  /**
   * Indicates if this fiber is suspended. The flag is initialized
   * with `false` since a new forked fiber is scheduled immediately
   * after creation. It becomes `true` when the fiber doesn't have
   * more tasks to execute. If new tasks are added later, the fiber
   * automatically reschedules itself in `add`.
   */
  private[this] val suspended = new AtomicBoolean(false)

  override protected[fiber] def hasExecutor = true

  /**
   * Given a forked fiber, all of its tasks will be under
   * the same trace so we can create the tracer once and
   * reuse it to improve performance so the trace Local
   * doesn't need to be read on each run.
   */
  private[this] val cpuTimeTracingStart = ForkedFiber.cpuTimeTracing()

  /**
   * Adds a new task and reschedules the fiber in case it
   * is detached.
   */
  override def add(r: Runnable, asOwner: Boolean): Unit = {
    super.add(r, asOwner)
    if (!asOwner) {
      // use a volatile read first (get) to avoid an
      // unnecessary full memory fence for the CAS call
      if (suspended.get() && suspended.compareAndSet(true, false)) {
        group.schedule(this)
      }
    }
  }

  /**
   * Runs the fiber
   */
  def run(worker: Worker): Boolean = {
    assert(!suspended.get())
    val stopHandler = worker.thread.stopHandler
    stopHandler.init(worker)

    val stop = cpuTimeTracingStart()
    runTasks(stopHandler)
    stop()

    var done = mailbox.isEmpty()
    if (done) {
      suspended.set(true)
      // accounts for the race condition when `runTasks`
      // finished but `suspended` is not set yet and there's
      // a concurrent `add` call
      if (!mailbox.isEmpty()) {
        done = !suspended.compareAndSet(true, false)
      }
    }
    if (done) {
      group.suspend(this)
    }
    done
  }

  override def toString =
    AvoidLoop(
      this,
      s"ForkedFiber(id = ${System.identityHashCode(this).toHexString}, suspended = $suspended, mailbox.itemsSize = ${mailbox.itemsSize}, mailbox.inboxSize = ${mailbox.inboxSize})",
      s"ForkedFiber(id = ${System.identityHashCode(this).toHexString})"
    )
}

private[fiber_scheduler] final object ForkedFiber {

  private val cpuTimeTracing =
    new CpuTimeTracing("forked_fiber", Config.Scheduling.cpuTimeTracingPercentage)

  /**
   * Handler that detects if a fiber execution should be stopped. The object is
   * stored in a thread local and reused to avoid allocations.
   * TODO check if it's possible to allocate the StopHandler on each execution instead of using a thread local
   */
  private[fiber_scheduler] final class StopHandler() extends (() => Boolean) {
    private[this] var workerMailbox: Mailbox[ForkedFiber] = null
    private[this] var deadline = 0L
    private[this] val slice = Config.Scheduling.fiberTimeSlice.inNanoseconds

    def init(worker: Worker) = {
      workerMailbox = worker.mailbox
      deadline = nowNanos() + slice
    }
    def apply() = {
      val preempt =
        if (deadline == 0) {
          true
        } else if (nowNanos() >= deadline) {
          // set the deadline to zero to avoid having
          // to check the time on every call
          deadline = 0
          true
        } else {
          false
        }
      preempt && !workerMailbox.isEmpty()
    }
  }
}
