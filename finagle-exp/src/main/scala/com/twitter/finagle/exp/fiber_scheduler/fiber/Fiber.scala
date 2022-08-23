package com.twitter.finagle.exp.fiber_scheduler.fiber

import scala.util.control.NonFatal
import com.twitter.util.Future
import com.twitter.util.Local
import com.twitter.util.Monitor
import com.twitter.util.Promise
import com.twitter.util.Try
import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.WorkerThread
import com.twitter.finagle.exp.fiber_scheduler.util.AvoidLoop
import com.twitter.finagle.exp.fiber_scheduler.util.Mailbox
import io.netty.util.internal.ThreadExecutorMap

/**
 * Base class of all fiber implementations.
 */
private[fiber_scheduler] abstract class Fiber {

  /**
   * Queue with the tasks of the fiber
   */
  protected[this] final val mailbox =
    Mailbox[Runnable](
      fifo = Config.Mailbox.fiberMailboxFifo,
      restricted = Config.Mailbox.fiberMailboxRestricted)

  private[this] var executions = 0
  @volatile private var blocked = false

  /**
   * Indicates if the fiber is backed by an executor.
   * Nested fibers use this information to switch the
   * execution back to a parent fiber if it has an executor
   */
  protected[fiber] def hasExecutor: Boolean

  /**
   * Adds a new task to the fiber. Ownership of the fiber
   * must be ensured by the caller
   */
  def add(r: Runnable, asOwner: Boolean): Unit = {
    if (asOwner) {
      mailbox.asOwnerAdd(r)
    } else {
      val ok = mailbox.asOtherAdd(r)
      assert(ok)
    }
  }

  /**
   * Submits a future computation to be executed by this fiber.
   * This method is currently called only once but a fiber
   * could support the execution of multiple future computations.
   */
  final def submit[T](fut: => Future[T], asOwner: Boolean, onCompletion: Int => Unit): Future[T] = {
    val promise = new Fiber.FiberPromise(this, fut, onCompletion)
    add(promise, asOwner)
    promise
  }

  /**
   * Flushes the fiber by executing its pending tasks
   */
  protected[fiber] final def flush(): Unit = {
    runTasks()
  }

  /**
   * Flushes the fiber before executing a blocking task.
   * Tasks submitted to this fiber while it's blocked
   * are executed by the caller since the fiber can't
   * make progress
   */
  private def blocking[T](task: => T): T = {
    flush()
    blocked = true
    try task
    finally {
      blocked = false
    }
  }

  def executions(): Int = executions

  /**
   * Runs tasks until no more tasks available
   */
  protected[this] final def runTasks(): Unit =
    runTasks(Fiber.neverStop)

  /**
   * Runs tasks until there are no more tasks available
   * or the stop function indicates that it should stop
   */
  protected[this] final def runTasks(stop: () => Boolean): Unit = {
    while (!stop()) {
      val r = mailbox.asOwnerPoll()
      if (r == null) {
        return
      }
      try r.run()
      catch Monitor.catcher
      executions += 1
    }
  }
}

/**
 * Fibers are managed using two mechanisms: a thread local and a future local.
 * The future local is used to persist the fiber information across async boundaries
 * and continue execution on the same fiber once the execution resumes. The thread
 * local indicates the fiber attached to the current thread.
 */
private[fiber_scheduler] final object Fiber {

  // avoids function allocation
  private val neverStop = () => false

  private[fiber_scheduler] val futureLocal = new Local[Fiber]
  private[fiber] val threadLocal = new ThreadLocal[Fiber]

  def add(r: Runnable): Unit = {
    var threadLocalFiber: Fiber = null
    var futureLocalFiberOpt: Option[Fiber] = None

    val wt = WorkerThread()
    if (wt != null) {
      // read values from the worker thread
      // if a worker is calling this method
      threadLocalFiber = wt.currentFiber
      futureLocalFiberOpt = wt.fiberLocalGetter()
    } else {
      // fall back in case a non-worker thread
      // is calling this method
      threadLocalFiber = threadLocal.get
      futureLocalFiberOpt = futureLocal()
    }

    val futureLocalFiber = {
      // don't use Option methods to avoid
      // allocations
      if (futureLocalFiberOpt eq None)
        null
      else
        futureLocalFiberOpt.get
    }

    if (futureLocalFiber != null && !futureLocalFiber.blocked) {
      // prioritize the fiber coming from the future local
      // it it's not blocked
      futureLocalFiber.add(r, asOwner = futureLocalFiber == threadLocalFiber)
    } else if (threadLocalFiber != null) {
      // use the fiber in the thread local if available
      threadLocalFiber.add(r, asOwner = true)
    } else {
      // create a new fiber if necessary
      var f: Fiber = null

      // try to reuse netty's executor
      if (Config.Scheduling.reuseNettyExecutor) {
        val exec = ThreadExecutorMap.currentExecutor()
        if (exec != null) {
          f = new ExecutorFiber(exec)
        }
      }
      if (f == null) {
        // fall back to a thread local fiber, which
        // functions similarly to `LocalScheduler.Activation`
        f = new ThreadLocalFiber
      }
      Fiber.threadLocal.set(f)
      f.add(r, asOwner = true)
    }
  }

  private[this] def currentFiber() = {
    WorkerThread() match {
      case null => threadLocal.get()
      case wt => wt.currentFiber
    }
  }

  /**
   * Used when `Scheduler.blocking` is called.
   */
  def blocking[T](task: => T): T = {
    val f = currentFiber()
    if (f != null) {
      f.blocking(task)
    } else {
      task
    }
  }

  /**
   * Used when `Scheduler.flush` is called.
   */
  def flush(): Unit = {
    val f = currentFiber()
    if (f != null) {
      f.flush()
    }
  }

  /**
   * Promise that represents a future computation submitted
   * to the fiber. A new class is used to merge multiple interfaces
   * and avoid allocations
   */
  private final class FiberPromise[T](
    fiber: Fiber,
    fut: => Future[T],
    onCompletion: Int => Unit)
      extends Promise[T]
      // Allows the promise to be submitted as a fiber task
      with Runnable
      // Completion callback
      with (Try[T] => Future[Unit]) {

    /**
     * Fiber from where the future computation was submitted.
     * Used to switch back to the parent on completion if it
     * has a backing executor
     */
    private[this] val parent = Fiber.threadLocal.get
    private[this] val saved = Local.save()

    /**
     * Triggers the future computation execution
     */
    override def run(): Unit = {
      val current = Local.save()
      if (current ne saved)
        Local.restore(saved)
      try {
        if (fiber.hasExecutor) {
          Fiber.futureLocal.set(Some(fiber))
        }
        val f =
          try fut
          catch {
            case NonFatal(ex) =>
              Future.exception(ex)
          }
        this.forwardInterruptsTo(f)
        f.transform(this)
      } catch Monitor.catcher
      finally {
        Local.restore(current)
      }
    }

    /**
     * Called once the future computation is completed.
     * The execution switches back to the parent fiber
     * if it's backed by an executor
     */
    override def apply(r: Try[T]) = {
      onCompletion(fiber.executions())
      if (parent != null && parent.hasExecutor) {
        Fiber.futureLocal.set(Some(parent))
      }
      this.update(r)
      Future.Unit
    }

    override def toString() =
      AvoidLoop(
        this,
        s"FiberPromise(id = ${System.identityHashCode(this).toHexString}, result = ${super.poll}, fiber = $fiber)",
        s"FiberPromise(id = ${System.identityHashCode(this).toHexString})"
      )
  }
}
