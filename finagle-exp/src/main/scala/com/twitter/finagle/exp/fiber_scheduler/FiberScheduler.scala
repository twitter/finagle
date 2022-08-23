package com.twitter.finagle.exp.fiber_scheduler

import java.util.ArrayList
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder
import com.twitter.concurrent.ForkingScheduler
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.finagle.exp.fiber_scheduler.fiber.ExecutorFiber
import com.twitter.finagle.exp.fiber_scheduler.fiber.Fiber
import com.twitter.finagle.exp.fiber_scheduler.fiber.ForkedFiber
import com.twitter.finagle.exp.fiber_scheduler.fiber.Group
import com.twitter.finagle.exp.fiber_scheduler.util.Optimizer
import com.twitter.finagle.exp.fiber_scheduler.util.PendingTracking
import com.twitter.finagle.exp.fiber_scheduler.util.XSRandom
import com.twitter.conversions.DurationOps._
import com.twitter.logging.Logger
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 *    ********************
 *    *** EXPERIMENTAL ***
 *    ********************
 *
 * The Fiber Scheduler uses fibers to implement a high-performance mechanism for
 * the execution of `Future` computations via load balancing, work stealing,
 * and continuation affinity.
 *
 * Fibers are created via the `fork` methods and they continue to be valid even after async boundaries.
 * All `Future` continuations of a fiber are grouped by execution within the fiber itself. Nested fibers
 * exchange continuations at their boundaries, providing a seamless mechanism to handle thread scheduling
 * of `Future` computations.
 *
 * This `Scheduler` can be used in parallel to other thread pools but it is also carefully
 * crafted to support all of an application's workload, including blocking and CPU-intensive
 * operations. If enabled with the default configuration, `FuturePool` tasks are automatically
 * redirected to the Fiber Scheduler.
 *
 * The adaptive size mechanism automatically adapts the number of worker threads to achieve peak
 * throughput and avoid known Aurora limits like CPU throttling and container memory usage.
 *
 * @param threads the initial number of threads
 * @param adaptiveSize indicates if the scheduler should adapt the number of threads automatically
 * @param minThreads min number of threads
 * @param maxThreads max number of threads
 */
final class FiberScheduler(
  threads: Int,
  adaptiveSize: Boolean,
  minThreads: Int,
  maxThreads: Int,
  maxQueuingDelay: Duration)
    extends ForkingScheduler {

  assert(threads > 0, "number of threads must be greater than zero")
  if (adaptiveSize) {
    assert(threads >= minThreads, "threads must be greater than minThreads")
    assert(threads <= maxThreads, "threads must be less than maxThreads")
    assert(maxThreads - minThreads > 3, "maxThreads - minThreads must be greater than 3")
  }

  private[this] val stats = new Stats
  private[this] val pending = new LongAdder
  private[this] val completions = new LongAdder
  private[this] val rejections = new LongAdder
  private[this] val workers = new CopyOnWriteArrayList[Worker]

  // Underlying thread pool used to execute workers. Tasks submitted to the scheduler
  // don't use this thread pool directly
  private[this] val threadPool =
    new ThreadPoolExecutor(
      minThreads,
      maxThreads,
      Config.Scheduling.threadExpiration.inMillis,
      TimeUnit.MILLISECONDS,
      new SynchronousQueue[Runnable](),
      WorkerThread.factory)

  // start workers
  try {
    val fut = Future.times(threads)(startWorker())
    Await.result(fut, 10.seconds)
  } catch {
    case NonFatal(ex) =>
      throw new IllegalStateException("Failed to start fiber scheduler workers", ex)
  }

  // start optimizer if adaptive size is true
  private[this] val optimizer: Option[Optimizer] =
    if (adaptiveSize) {
      Some(
        FiberSchedulerOptimizer(
          minThreads,
          maxThreads,
          startWorker,
          stopWorker,
          threadPool,
          completions,
          rejections,
          workers))
    } else {
      None
    }

  /**
   * Forks a `Future` computation. A `ForkedFiber` is created
   * to execute the task and all of its `Future` continuations,
   * including after async boundaries.
   *
   * Typically called from redirected `FuturePools`. Users can
   * also call this method directly.
   *
   * If called from a parent fiber that is backed by an executor
   * (`ForkedFiber` or `ExecutorFiber`), the execution switches
   * back to the parent fiber once the computation finishes.
   *
   * This method doesn't reject execution even if the scheduler
   * is overloaded
   */
  override def fork[T](fut: => Future[T]): Future[T] = {
    fork(defaultGroup)(fut)
  }

  private def fork[T](group: Group)(fut: => Future[T]): Future[T] = {
    val f = new ForkedFiber(group)
    val p = f.submit(fut, asOwner = true, onCompletion)
    group.schedule(f)
    pending.increment()
    p
  }

  /**
   * Tries to fork a `Future` computation and returns None if the scheduler
   * is overloaded. If the scheduler is not overloaded, a `ForkedFiber`
   * is created to execute the task and all of its `Future` continuations,
   * including after async boundaries.
   *
   * Typically called from the `ForkingSchedulerFilter`. Users can also
   * call this method directly.
   *
   * If called from a parent fiber that is backed by an executor
   * (`ForkedFiber` or `ExecutorFiber`), the execution switches
   * back to the parent fiber once the computation finishes.
   *
   * This method can reject execution if the scheduler is overloaded.
   */
  override def tryFork[T](fut: => Future[T]): Future[Option[T]] = {
    tryFork(defaultGroup)(fut)
  }

  private def tryFork[T](group: Group)(fut: => Future[T]): Future[Option[T]] = {
    val f = new ForkedFiber(group)
    val p = f.submit(fut, asOwner = true, onCompletion)
    if (!group.trySchedule(f)) {
      rejections.increment()
      Future.None
    } else {
      pending.increment()
      p.map(Some(_))
    }
  }

  /**
   * Executes the computation using the provided executor. An `ExecutorFiber`
   * is created to schedule the execution of the task and its `Future` continuations
   * on the provided executor.
   */
  override def fork[T](executor: Executor)(fut: => Future[T]): Future[T] = {
    val f = new ExecutorFiber(executor)
    val p = f.submit(fut, asOwner = false, onCompletion)
    pending.increment()
    p
  }

  private[this] final val onCompletion = (executions: Int) => {
    stats.executionsPerFiber.add(executions)
    completions.increment()
    pending.decrement()
  }

  /**
   * Implements the `Scheduler` interface used to execute `Future` continuations.
   */
  override def submit(r: Runnable): Unit =
    Fiber.add(r)

  /**
   * Creates a wrapper to expose the scheduler as an `ExecutorService`.
   * The wrapper can be shutdown but the underlying scheduler remains unchanged.
   */
  override def asExecutorService(): ExecutorService =
    FiberScheduler.asExecutorService(this)

  override def withMaxSyncConcurrency(concurrency: Int, maxWaiters: Int): ForkingScheduler =
    FiberScheduler.withMaxSyncConcurrency(this, concurrency, maxWaiters)

  /**
   * Indicates if tasks submitted through `FuturePool` instances should
   * be directed to this scheduler. See `FuturePool.apply` for more information.
   */
  override def redirectFuturePools() =
    Config.Scheduling.redirectFuturePools

  /**
   * Forces the execution of the current fiber's pending tasks. No-op if
   * called outside of a fiber.
   */
  def flush(): Unit = Fiber.flush()
  def numDispatches: Long = completions.sum()
  def blocking[T](f: => T)(implicit perm: Awaitable.CanAwait): T = Fiber.blocking(f)

  /**
   * Tries to schedule the execution of the fiber by a worker. The
   * number of tries is limited by `Config.Scheduling.scheduleTries`.
   *
   * On each try, the impl chooses two random workers and prioritizes
   * the one that has lower load.
   */
  private val trySchedule: ForkedFiber => Boolean =
    (f: ForkedFiber) => {
      assert(workers.size() >= 1)
      val maxTries = Config.Scheduling.scheduleTries

      @tailrec def loop(tries: Int): Boolean =
        (tries < maxTries) && {
          var a = randomWorker()
          var b = randomWorker()
          if (a.load() > b.load()) {
            val tmp = a
            a = b
            b = tmp
          }
          a.enqueue(f, force = false) ||
          b.enqueue(f, force = false) ||
          loop(tries + 1)
        }

      loop(tries = 0)
    }

  /**
   * Schedules a fiber for execution after it returns from a detached state
   * due to a new `submit` task. This method doesn't reject execution and
   * forces the enqueuing of the fiber if necessary.
   *
   * On each try, the impl chooses two random workers and prioritizes
   * the one that has lower load.
   */
  private val schedule: ForkedFiber => Unit =
    (f: ForkedFiber) => {
      assert(workers.size() >= 1)
      // force the enqueuing after this threshold
      val forceThreshold = Config.Scheduling.scheduleTries / 2

      @tailrec def loop(tries: Int, force: Boolean): Unit = {
        var a = randomWorker()
        var b = randomWorker()
        if (a.load() > b.load()) {
          val tmp = a
          a = b
          b = tmp
        }
        if (!a.enqueue(f, force) && !b.enqueue(f, force)) {
          loop(tries + 1, tries >= forceThreshold)
        }
      }

      loop(tries = 0, force = false)
    }

  private[this] val defaultGroup = Group.default(trySchedule, schedule)

  /**
   * Tries to steal a fiber. The number of tries is limited by
   * `Config.Scheduling.stealTries`.
   *
   * On each try, the impl chooses two random workers and prioritizes
   * the one that has higher load.
   */
  private[this] def stealFiber(): ForkedFiber = {
    assert(workers.size() >= 1)
    val maxTries = Config.Scheduling.stealTries

    @tailrec def loop(tries: Int): ForkedFiber = {
      var a = randomWorker()
      var b = randomWorker()
      if (a.load() < b.load()) {
        val tmp = a
        a = b
        b = tmp
      }
      var stolen = a.steal()
      if (stolen == null) {
        stolen = b.steal()
      }
      if (tries < maxTries - 1 && stolen == null) {
        loop(tries + 1)
      } else {
        stolen
      }
    }

    loop(tries = 0)
  }

  /**
   * Requests a worker stop and returns a `Promise`
   * to be fulfilled once a worker is stopped.
   */
  private[this] lazy val stopWorker = () => {
    val p = Promise[Unit]()
    @tailrec def tryStop(cond: Worker => Boolean, i: Int = 0): Boolean = {
      (i < workers.size()) && {
        val w = workers.get(i)
        (cond(w) && w.stop(p)) || tryStop(cond, i + 1)
      }
    }
    if (!tryStop(_.load() == 0) && !tryStop(_ => true)) {
      p.setException(new IllegalStateException("Can't find a worker to stop"))
    }
    p
  }

  /**
   * Starts a worker and returns a `Promise` to be fulfilled
   * once the worker is running.
   */
  private[this] lazy val startWorker = () => {
    val started = Promise[Unit]
    threadPool.submit(new Runnable {
      override def run() = {
        try {
          val w = new Worker(stealFiber, maxQueuingDelay, WorkerThread())
          workers.add(w)
          started.setDone()
          var backlog = w.run()
          workers.remove(w)
          // reschedule the backlog
          while (backlog ne Nil) {
            val fiber = backlog.head
            fiber.group.activate(fiber)
            backlog = backlog.tail
          }
          w.cleanup()
        } catch {
          case ex: Throwable =>
            FiberScheduler.log.error("Fiber scheduler worker failed!", ex)
            started.setException(ex)
        }
      }
    })
    started
  }

  @tailrec private[this] def randomWorker(): Worker = {
    // It is possible that a worker is removed between the `workers.size`
    // and `workers.get` calls. Instead of using a synchronization mechanism
    // to avoid that, it's cheaper to retry here since it's a rare edge case
    workers.size() match {
      case 0 =>
        throw new IllegalStateException(
          "Can't find a random worker because the workers list is empty")
      case size =>
        try workers.get(XSRandom.nextInt(size))
        catch {
          case _: ArrayIndexOutOfBoundsException =>
            randomWorker()
        }
    }
  }

  /**
   * Shuts down the scheduler. Meant to be used for testing purposes only.
   */
  private[fiber_scheduler] def shutdown(): Future[Unit] = {
    // stop the optimizer
    optimizer.foreach(_.stop())
    // stop gauges
    stats.remove()
    // stop all workers
    var l = List.empty[Promise[Unit]]
    workers.forEach { w =>
      val p = Promise[Unit]()
      if (w.stop(p)) {
        l ::= p
      }
    }
    Future.collect(l).unit
  }

  private[this] final class Stats {
    val executionsPerFiber = statsReceiver.stat("executions_per_fiber")
    private[this] val poolStats = statsReceiver.scope("pool")
    private[this] val gauges = List(
      statsReceiver.addGauge("workers_size")(workers.size()),
      statsReceiver.addGauge("completions")(completions.sum()),
      statsReceiver.addGauge("pending")(pending.sum()),
      statsReceiver.addGauge("rejections")(rejections.sum()),
      poolStats.addGauge("size")(threadPool.getPoolSize),
      poolStats.addGauge("largest_size")(threadPool.getLargestPoolSize),
      poolStats.addGauge("active_tasks")(threadPool.getActiveCount),
      poolStats.addGauge("completed_tasks")(threadPool.getCompletedTaskCount)
    )
    def remove() = gauges.foreach(_.remove())
  }

  override def toString = {
    import scala.collection.JavaConverters._
    s"FiberScheduler(workers = ${workers.size}, adaptiveSize = $adaptiveSize, completions = $completions)\n\t${workers
      .iterator().asScala.mkString("\n\t")}"
  }
}

final object FiberScheduler {

  private val log = Logger()

  /**
   * Creates an `ExecutorService` wrapper for a `ForkingScheduler`.
   */
  private[fiber_scheduler] def asExecutorService(s: ForkingScheduler): ExecutorService =
    new AbstractExecutorService {
      private[this] val isShutdown = new AtomicBoolean(false)
      override def execute(r: Runnable) = {
        if (isShutdown.get()) {
          throw new RejectedExecutionException("Terminated")
        }
        s.fork(Future.value(r.run()))
      }
      override def isShutdown(): Boolean = isShutdown.get()
      override def awaitTermination(timeout: Long, unit: TimeUnit) = isShutdown.get()
      override def isTerminated(): Boolean = isShutdown.get()
      override def shutdown(): Unit = isShutdown.set(true)
      override def shutdownNow() = {
        shutdown()
        new ArrayList()
      }
    }

  /**
   * Creates a wrapper of `ForkingScheduler` with a fixed max concurrency.
   */
  private def withMaxSyncConcurrency(
    s: FiberScheduler,
    concurrency: Int,
    maxWaiters: Int
  ): ForkingScheduler =
    if (concurrency == Int.MaxValue) {
      s
    } else {
      require(concurrency > 0, "concurrency must be greater than zero")
      require(maxWaiters > 0, "maxWaiters must be greater than zero")
      val group = Group.withMaxSyncConcurrency(concurrency, maxWaiters, s.trySchedule, s.schedule)
      new DelegatingForkingScheduler(s) {
        override def tryFork[T](f: => Future[T]): Future[Option[T]] =
          s.tryFork(group)(f)
        override def fork[T](f: => Future[T]) =
          s.fork(group)(f)
      }
    }

  /**
   * Creates a `ForkingScheduler` wrapper that tracks pending tasks.
   */
  def withPendingTracking(s: ForkingScheduler): ForkingScheduler = {
    val forkTracking = PendingTracking.ForFuture(
      s,
      "fork",
      Config.PendingTracking.forkSamples,
      Config.PendingTracking.forkThreshold)
    val submitTracking = PendingTracking.ForRunnable(
      s,
      "submit",
      Config.PendingTracking.submitSamples,
      Config.PendingTracking.submitThreshold)

    new DelegatingForkingScheduler(s) {
      override def submit(r: Runnable) =
        s.submit(submitTracking(r))
      override def fork[T](f: => Future[T]) =
        forkTracking(s.fork(f))
      override def tryFork[T](f: => Future[T]) =
        forkTracking(s.tryFork(f))
      override def fork[T](executor: Executor)(fut: => Future[T]) =
        forkTracking(s.fork(executor)(fut))
    }
  }
}
