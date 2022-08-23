package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.logging.Logger
import com.twitter.util.Duration
import com.twitter.util.Promise
import com.twitter.util.Stopwatch
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import com.twitter.finagle.exp.fiber_scheduler.fiber.ForkedFiber
import com.twitter.finagle.exp.fiber_scheduler.util.AvoidLoop
import com.twitter.finagle.exp.fiber_scheduler.util.Mailbox
import com.twitter.finagle.exp.fiber_scheduler.WorkerThroughput.timer
import java.util.concurrent.atomic.AtomicReference

/**
 * Workers are responsible for executing `ForkedFiber`s.
 *
 * @param continue indicates if the worker should continue executing tasks or stop and exit
 * @param stealFiber function used by the worker to try to steal a fiber from other workers
 * @param maxQueuingDelay if the queuing delay is greater than this limit, the worker can
 *                        reject new fibers
 * @param thread the thread that is responsible for running this worker. The thread object
 *               has fields that are mutated freely by the worker without synchronization
 *               similarly to how thread locals are used.
 */
private final class Worker(
  stealFiber: () => ForkedFiber,
  maxQueuingDelay: Duration,
  val thread: WorkerThread) {

  private[this] val stats = new Stats

  // Execution queue of the worker
  private[fiber_scheduler] val mailbox =
    Mailbox[ForkedFiber](
      fifo = Config.Mailbox.workerMailboxFifo,
      restricted = Config.Scheduling.stealTries == 0 || Config.Mailbox.workerMailboxRestricted)

  private[this] val load = new AtomicInteger(0)
  private[this] val stopPromiseRef = new AtomicReference[Promise[Unit]]()

  @volatile private[this] var executions = 0L
  private[this] val throughput = WorkerThroughput(executions)
  private[this] val maxDelayMs = maxQueuingDelay.inMillis

  // if not null, indicates that the worker is parked or attempting to park
  @volatile private[this] var parkedThread: Thread = null

  /**
   * Tries to stop this worker. If this method returns
   * `true`, the promise will be satisfied once the worker
   * finishes its stop flow. If `false`, the promise is
   * unused.
   */
  def stop(p: Promise[Unit]): Boolean = {
    val r = stopPromiseRef.compareAndSet(null, p)
    if (r) unpark()
    r
  }

  /**
   * Load of the worker. Considers the number of enqueued fibers + the fiber
   * under execution (if there's one).
   */
  def load(): Int =
    load.get()

  /**
   * Unparks the worker's thread. No-op if the worker is not parked.
   */
  def unpark(): Unit =
    LockSupport.unpark(parkedThread)

  /**
   * Estimates the queuing delay based on the throughput of the
   * worker and its load
   */
  private[this] def queuingDelay(load: Int): Double =
    if (load == 0) 0D
    else {
      val t = throughput.perMs()
      if (t == 0) {
        // An unreasonable delay but still less than Duration.Top.inMillis
        Int.MaxValue.asInstanceOf[Double]
      } else {
        // use `asInstanceOf` to avoid Scala's implicit conversion that happens with `toDouble`
        load.asInstanceOf[Double] / t
      }
    }

  /**
   * Tries to enqueue the fiber for execution. This method doesn't implement retries, which is
   * handled by the caller (see logic in `FiberScheduler.[trySchedule|schedule]`). For example,
   * if two threads try to enqueue to the same worker concurrently, only one will succeed. This behavior
   * allows for a more precise control of the queuing delay since if it allowed concurrent enqueuing,
   * the estimation of the delay could be skewed. It also contributes to a better distribution of the
   * workload by giving an opportunity to the caller to select another worker. If the load on this
   * worker has changed concurrently, it'll have more fibers than when the fiber scheduler initially
   * decided to select it for enqueueing.
   *
   * @param fiber the fiber to be enqueued
   * @param force if true, the max queuing limit is ignored
   * @return if the enqueuing is successful
   */
  def enqueue(fiber: ForkedFiber, force: Boolean): Boolean = {
    val proceed =
      if (force) {
        load.incrementAndGet()
        true
      } else {
        val l = load.get()
        queuingDelay(l) <= maxDelayMs &&
        load.compareAndSet(l, l + 1)
      }
    if (proceed) {
      if (mailbox.asOtherAdd(fiber)) {
        unpark()
        true
      } else {
        load.decrementAndGet()
        stats.rejections += 1
        false
      }
    } else {
      stats.rejections += 1
      false
    }
  }

  /**
   * Main worker execution loop. Stops once `continue()` returns `false`.
   * @return the backlog of the worker once its stops.
   */
  def run(): List[ForkedFiber] = {
    thread.currentWorker = this
    var spins = 0
    val maxSpins = Config.Scheduling.workerMaxSpins

    // tries to park the worker's thread
    def park() = {
      parkedThread = thread
      // accounts for race condition in `enqueue` when `parkedThread`
      // is not set yet and a new item is added to the mailbox
      if (mailbox.isEmpty()) {
        stats.parks += 1
        // even though microbenchmarks might show that not parking
        // indefinitely can improve throughput, tests in production
        // show that the CPU cost of repeatedly parking and unparking
        // threads is significant
        LockSupport.park(this)
      }
      parkedThread = null
    }

    def resetSpins() = {
      if (spins > 0) {
        stats.successfulSpins += 1
      }
      spins = 0
    }

    while (stopPromiseRef.get() == null) {
      val f = mailbox.asOwnerPoll()
      if (f != null) {
        run(f)
        resetSpins()
      } else {
        val f = stealFiber()
        if (f != null) {
          load.incrementAndGet()
          stats.steals += 1
          run(f)
          resetSpins()
        } else if (spins < maxSpins) {
          stats.spins += 1
          spins += 1
        } else if (stopPromiseRef.get() == null) {
          park()
          spins = 0
        }
      }
    }

    // return the backlog of pending fibers
    mailbox.asOwnerClose()
  }

  /**
   * Cleanup resources used by this worker
   * after it's already shutdown
   */
  def cleanup(): Unit = {
    thread.currentWorker = null
    stats.remove()
    throughput.stop()
    stopPromiseRef.get().setDone
  }

  /**
   * Runs a fiber and re-enqueues it for execution
   * if it was preempted.
   */
  private[this] def run(f: ForkedFiber) = {
    thread.currentFiber = f
    val done = f.run(this)
    thread.currentFiber = null
    if (done) {
      load.decrementAndGet()
    } else {
      stats.preemptions += 1
      mailbox.asOwnerAddLast(f)
    }
    executions += 1
  }

  /**
   * Tries to steal a fiber from the mailbox.
   */
  def steal(): ForkedFiber = {
    var f: ForkedFiber = null
    if (load.get > 1) {
      f = mailbox.asOtherSteal()
      if (f != null) {
        load.decrementAndGet()
        stats.stolen += 1
      }
    }
    f
  }

  /**
   * Indicates if the worker is running a blocking operation.
   * The blocker object is checked to avoid considering a parked
   * worker as blocked.
   */
  def isBlocked =
    thread.currentFiber != null &&
      thread.getState != Thread.State.RUNNABLE &&
      LockSupport.getBlocker(thread) != this

  /**
   * Uses non-volatile counters to avoid memory barriers
   * in hot code. The values reported by the gauges can be stale
   * if the worker thread hasn't reached a write barrier yet
   * but the impact of that for metrics should minimal.
   */
  private[this] final class Stats {
    private[this] val id = Worker.StatsId.gen()
    private[this] val uptime = Stopwatch.start()

    var steals = 0L
    var stolen = 0L
    var rejections = 0L
    var preemptions = 0L
    var parks = 0L
    var spins = 0L
    var successfulSpins = 0L

    private[this] val scope = statsReceiver.scope("workers", id.toString())

    private[this] val gauges = List(
      scope.addGauge("uptimeMs")(uptime().inMillis),
      scope.addGauge("steals")(steals),
      scope.addGauge("stolen")(stolen),
      scope.addGauge("executions")(executions),
      scope.addGauge("rejections")(rejections),
      scope.addGauge("preemptions")(preemptions),
      scope.addGauge("parks")(parks),
      scope.addGauge("spins")(spins),
      scope.addGauge("successful_spins")(successfulSpins),
      scope.addGauge("load")(load.get),
      scope.addGauge("throughput")(throughput.perMs().toFloat),
      scope.addGauge("queuing_delay")(queuingDelay(load.get).toFloat),
      scope.addGauge("thread_state")(thread.getState.ordinal())
    )

    def remove() = {
      Worker.StatsId.release(id)
      gauges.foreach(_.remove)
    }
  }

  override def toString = AvoidLoop(
    this,
    s"Worker(${thread.getName}, load=$load, throughput=${throughput
      .perMs()}, currentFiber=${thread.currentFiber}, mailbox=$mailbox, threadState=${thread.getState})",
    s"Worker(${thread.getName})"
  )
}

private[fiber_scheduler] final object Worker {

  private val log = Logger()

  // Mechanism to assign IDs to workers. It reuses IDs to avoid
  // metrics explosion.
  private final object StatsId {
    private[this] val nextId = new AtomicInteger
    private[this] val releasedIds = new ConcurrentSkipListSet[Integer]

    def gen(): Int = {
      var i = releasedIds.pollFirst()
      if (i == null) {
        i = nextId.getAndIncrement
      }
      i
    }

    def release(i: Int): Unit = {
      releasedIds.add(i)
    }
  }
}
