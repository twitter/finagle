package com.twitter.finagle.exp

import com.twitter.concurrent.{LocalScheduler, Scheduler}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.Awaitable.CanAwait
import com.twitter.util.Monitor
import java.util.concurrent.{ForkJoinPool, ForkJoinTask, ForkJoinWorkerThread}
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicLong

/**
 * A scheduler based on the JSR166 ForkJoin pool. In addition
 * to balancing load by work-stealing, it implements managed
 * blocking to ensure desired parallelism is retained.
 */
private class ForkJoinScheduler(nthreads: Int, statsReceiver: StatsReceiver = NullStatsReceiver)
    extends Scheduler {
  private trait IsManagedThread

  private[this] val numBlocks = statsReceiver.counter("blocks")
  private[this] val activeBlocks = new AtomicLong(0L)
  private[this] val threadsMade = statsReceiver.counter("threads_made")
  private[this] val threadCount = new AtomicLong(0L)
  private[this] val splitCount = new AtomicLong(0L)

  private[this] val local = new LocalScheduler

  private[this] val threadFactory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
    def newThread(pool: ForkJoinPool) = {
      val thread = new ForkJoinWorkerThread(pool) with IsManagedThread
      thread.setName("Finagle ForkJoin Worker #" + (threadCount.getAndIncrement()))
      thread.setDaemon(true)
      threadsMade.incr()
      thread
    }
  }

  private[this] val exceptionHandler = new Thread.UncaughtExceptionHandler {
    def uncaughtException(t: Thread, exc: Throwable): Unit = {
      Monitor.handle(exc)
    }
  }

  private[this] val pool = new ForkJoinPool(
    nthreads,
    threadFactory,
    exceptionHandler,
    true /*async mode*/
  )

  private[this] val gauges = Seq(
    // The number of currently active managed blocking operations.
    statsReceiver.addGauge("active_blocks") { activeBlocks.get },
    // Returns an estimate of the number of threads that are
    // currently stealing or executing tasks.
    statsReceiver.addGauge("active_threads") { pool.getActiveThreadCount() },
    // Returns the targeted parallelism level of this pool.
    statsReceiver.addGauge("parallelism") { pool.getParallelism() },
    // Returns the number of worker threads that have started
    // but not yet terminated.
    statsReceiver.addGauge("pool_size") { pool.getPoolSize() },
    // Returns an estimate of the number of tasks submitted to this
    // pool that have not yet begun executing.
    statsReceiver.addGauge("queued_submissions") { pool.getQueuedSubmissionCount() },
    // Returns an estimate of the total number of tasks currently
    // held in queues by worker threads (but not including tasks
    // submitted to the pool that have not begun executing).
    statsReceiver.addGauge("queued_tasks") { pool.getQueuedTaskCount() },
    // Returns an estimate of the number of worker threads that are not
    // blocked waiting to join tasks or for other managed synchronization.
    statsReceiver.addGauge("running_threads") { pool.getRunningThreadCount() },
    // Returns an estimate of the total number of tasks stolen from one thread's
    // work queue by another.
    statsReceiver.addGauge("steals") { pool.getStealCount() },
    // The number of tasks that were split off a local schedule.
    statsReceiver.addGauge("splits") { splitCount.get }
  )

  def submit(r: Runnable): Unit = {
    Thread.currentThread() match {
      case t: ForkJoinWorkerThread if t.getPool eq pool =>
        local.submit(r)

      case _ =>
        try pool.execute(ForkJoinTask.adapt(r))
        catch {
          // ForkJoin pools reject execution only when its internal
          // resources are exhausted. It is a serious, nonrecoverable
          // error.
          case cause: RejectedExecutionException =>
            throw new Error("Resource exhaustion in ForkJoin pool", cause)
        }
    }
  }

  private def flushLocalScheduler(): Unit = {
    var n = 0
    while (local.hasNext) {
      ForkJoinTask.adapt(local.next()).fork()
      n += 1
    }
    if (n > 0) splitCount.addAndGet(n)
  }

  def blocking[T](f: => T)(implicit perm: CanAwait): T = {
    Thread.currentThread() match {
      case _: IsManagedThread =>
        // Flush out our local scheduler before proceeding.
        flushLocalScheduler()

        var res: T = null.asInstanceOf[T]
        ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker {
          @volatile private[this] var ok = false
          override def block(): Boolean = {
            numBlocks.incr()
            activeBlocks.incrementAndGet()
            res =
              try f
              finally {
                ok = true
                activeBlocks.decrementAndGet()
              }
            true
          }
          override def isReleasable: Boolean = ok
        })
        res

      case _ =>
        // There's nothing we can do.
        f
    }
  }

  def flush(): Unit = Thread.currentThread() match {
    case _: IsManagedThread =>
      // Flush out our local scheduler before proceeding.
      flushLocalScheduler()
    case _ =>
    // Nothing to do.
  }

  // We can't provide useful/cheap implementations of these.
  def numDispatches: Long = -1L
}
