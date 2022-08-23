package com.twitter.finagle.exp.fiber_scheduler.fiber

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal
import com.twitter.logging.Logger
import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.util.AvoidLoop

/**
 * A fiber implementation that relies on an `Executor` to perform
 * tasks. Used by `FiberScheduler.fork` when an executor is specified
 * and by `Fiber.add` as a way to reuse netty's thread pool.
 */
private[fiber_scheduler] final class ExecutorFiber(exec: Executor) extends Fiber {

  private[this] val running = new AtomicBoolean(false)

  override protected[fiber] def hasExecutor = true

  /**
   * Adds a new task and triggers execution by the `Executor`
   * if it's not already running
   */
  override def add(r: Runnable, asOwner: Boolean): Unit = {
    super.add(withCpuTracing(r), asOwner)
    if (!running.get && running.compareAndSet(false, true)) {
      try exec.execute(loop)
      catch {
        case NonFatal(ex) =>
          ExecutorFiber.log.warning(
            s"Failed to submit task to $exec. Using current thread to run tasks (${Thread.currentThread()})",
            ex)
          loop.run()
      }
    }
  }

  /**
   * Since the executor can execute tasks in any order and
   * can have multiple tasks for different traced requests
   * in its queue, wrap each task to record cpu tracing if
   * it's enabled. There's a higher overhead but executor
   * tasks are currently used only as a bridge to interact
   * with netty threads and each request typically has a
   * single executor task for each interaction with netty
   * (async boundaries), so it's a low rate of tasks.
   * This mechanism might need review if we start using
   * executor fibers for other things.
   */
  private[this] def withCpuTracing(r: Runnable): Runnable = {
    val start = ExecutorFiber.cpuTimeTracing()
    if (start eq CpuTimeTracing.nullStart) {
      r
    } else { () =>
      val stop = start()
      r.run()
      stop()
    }
  }

  private[this] val loop: Runnable = () =>
    do {
      Fiber.threadLocal.set(this)
      runTasks()
      Fiber.threadLocal.remove()
      running.set(false)
    } while (continue())

  // accounts for race in `wakeUp` when `running.get` returns true
  // and `run` is finishing but hasn't set `running` to `false` yet
  private[this] def continue() =
    !mailbox.isEmpty() && running.compareAndSet(false, true)

  override def toString =
    AvoidLoop(
      this,
      s"ExecutorFiber(exec = $exec, mailbox.itemsSize = ${mailbox.itemsSize}, mailbox.inboxSize = ${mailbox.inboxSize})",
      s"ExecutorFiber(exec = $exec)"
    )
}

final object ExecutorFiber {
  private val log = Logger()
  private val cpuTimeTracing =
    new CpuTimeTracing("executor_fiber", Config.Scheduling.cpuTimeTracingPercentage)
}
