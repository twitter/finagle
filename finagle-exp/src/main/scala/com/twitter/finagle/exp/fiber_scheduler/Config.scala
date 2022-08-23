package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.app.Flaggable
import com.twitter.conversions.DurationOps._
import com.twitter.logging.Logger
import com.twitter.util.Duration

/**
 * This object contains all configuration parameters of the Fiber
 * Scheduler. The parameters are read from system properties at
 * startup and remain unchanged during execution. System property
 * format: `com.twitter.finagle.scheduler.$scope.$name`
 *
 * It doesn't use `GlobalFlag` since its implementation has
 * a significant performance overhead and a full memory fence on each
 * read, making it problematic if used in a hot code path.
 */
private[fiber_scheduler] final object Config {

  private final val log = Logger()

  /**
   * Indicates the scenarios in which a flag could require tuning.
   * It's only informative and isn't automatically checked.
   */
  sealed trait Usage
  object Usage {

    /**
     * Flags that are expected to need eventual tuning and can be
     * configured to different values in production
     */
    case object Production extends Usage

    /**
     * Flags that shouldn't require tuning but are made available
     * to make it easier identify potential fixes and optimizations
     * in a staging environment.
     */
    case object Staging extends Usage

    /**
     * Debugging flags. If changed in production, the deploy must
     * be limited to a canary or a small set of app instances.
     */
    case object Debug extends Usage
  }

  /**
   * Flags to change the scheduling behavior. The defaults are expected to work
   * well with different kinds of workloads but corner cases could require tuning.
   */
  final object Scheduling {
    private val params = Scope("scheduling")

    // Production flags

    // the workers estimate their queuing delay and reject new fibers
    // if it's above this limit.
    val maxQueuingDelay = params(Usage.Production, "maxQueuingDelay", 10.millis)

    // can be used to enable the redirect of `FuturePool` tasks to the
    // Fiber Scheduler
    val redirectFuturePools = params(Usage.Production, "redirectFuturePools", true)

    // the time slice after which the scheduler should preempt the fiber
    val fiberTimeSlice = params(Usage.Production, "fiberTimeSlice", 20.millis)

    // percentage of the traced requests that will include cpu time tracking
    val cpuTimeTracingPercentage = params(Usage.Production, "cpuTimeTracingPercentage", 0)

    // Staging flags

    // netty executors are reused to execute fibers by default
    val reuseNettyExecutor = params(Usage.Staging, "reuseNettyExecutor", true)

    // thread expiration of the underlying thread pool used by the workers
    val threadExpiration = params(Usage.Staging, "threadExpiration", 5.minutes)

    // number of tries to schedule new fibers. Once this limit is reached,
    // the execution is rejected. Must be greater than zero.
    val scheduleTries = params(Usage.Staging, "scheduleTries", 32)

    // number of times that workers should try to steal tasks. Must be greater than zero.
    val stealTries = params(Usage.Staging, "stealTries", 1)

    // interval between throughput measurements
    val workerThroughputInterval = params(Usage.Staging, "workerThroughputInterval", 10.millis)
    // the number of measurements to consider when calculating the throughput
    // for example, if it's 32 and the interval is 10 millis, measurements of
    // the past 320 millis will be considered
    val workerThroughputWindow = params(Usage.Staging, "workerThroughputWindow", 32)

    // number of times a worker should spin before parking
    val workerMaxSpins = params(Usage.Staging, "workerMaxSpins", 64)

    // the resolution of the clock used to contol the time slice
    val lowResClockResolution = params(Usage.Staging, "lowResClockResolution", 5.millis)
  }

  /**
   * Optimizer configurations. The Production flags can be used to change
   * how aggressive the optimizer is in case the defaults aren't sensible
   * enough.
   */
  final object Optimizer {
    private val params = Scope("optimizer")

    // Production flags

    // the optimizer doesn't make changes if the percentage of active workers
    // is smaller than this threshold.
    val idleThresholdPercent = params(Usage.Production, "idleThresholdPercent", 5)

    // the memory cliff is useful if the aurora container is running out of
    // memory because of the off heap allocation of thread stacks.
    val memoryCliffMaxUsagePercent =
      params(Usage.Production, "memoryCliffMaxUsagePercent", 99.9999999999)

    // number of periods to make measurements before making an adapt decision
    val optimizerAdaptPeriod = params(Usage.Production, "optimizerAdaptPeriod", 32)

    // number of periods before chaging the phase of the optimizer
    val optimizerWavePeriod = params(Usage.Production, "optimizerWavePeriod", 8)

    // once the cliffs are detected, they stay valid until they're expired.
    // these configs can be changed in case the optimizer doesn't react fast
    // enough to incoming load changes.
    val cliffExpiration = params(Usage.Production, "cliffExpiration", 1.minute)
    val valleyExpiration = params(Usage.Production, "valleyExpiration", 1.minute)

    // Staging flags

    // the default optimizer cycle in case the scheduler can't read the cpu period
    // from the cgroup configuration. Shouldn't have an effect in aurora.
    val optimizerDefaultCycle = params(Usage.Staging, "optimizerDefaultCycle", 100.millis)

    // flags to limit the number of times that cliffs and valleys should be applied

    val memoryCliffMaxTries = params(Usage.Staging, "memoryCliffMaxTries", 5)
    val memoryCliffMaxTriesExpiration =
      params(Usage.Staging, "memoryCliffMaxTriesExpiration", 5.seconds)
    val cpuThrottlingCliffMaxTries = params(Usage.Staging, "cpuThrottlingCliffMaxTries", 10)
    val cpuThrottlingCliffMaxTriesExpiration =
      params(Usage.Staging, "cpuThrottlingCliffMaxTriesExpiration", 1.seconds)

    val blockedWorkersValleyMaxPercent =
      params(Usage.Production, "blockedWorkersValleyMaxPercent", 90D)
    val blockedWorkersValleyMaxTries = params(Usage.Staging, "blockedWorkersValleyMaxTries", 10)
    val blockedWorkersValleyMaxTriesExpiration =
      params(Usage.Staging, "blockedWorkersValleyMaxTriesExpiration", 1.seconds / 4)

    val rejectionsValleyMaxTries = params(Usage.Staging, "rejectionsValleyMaxTries", 10)
    val rejectionsValleyMaxTriesExpiration =
      params(Usage.Staging, "rejectionsValleyMaxTriesExpiration", 1.seconds)
  }

  /**
   * Pending tracking is a low-overhead debugging utility. It samples the tasks
   * submitted to the Fiber Scheduler to report tasks that are never executed
   * or don't terminate.
   */
  final object PendingTracking {
    private val params = Scope("pendingTracking")

    // Debug flags

    val enabled = params(Usage.Debug, "enabled", false)

    // reporting interval
    val interval = params(Usage.Debug, "interval", 1.minutes)

    // max number of `fork` and `submit` calls to be sampled in parallel
    val forkSamples = params(Usage.Debug, "forkSamples", 5)
    val submitSamples = params(Usage.Debug, "submitSamples", 5)

    // threshold to consider a task pending
    val forkThreshold = params(Usage.Debug, "forkThreshold", 30.seconds)
    val submitThreshold = params(Usage.Debug, "submitThreshold", 30.seconds)
  }

  /**
   * Flags that change the behavior of the Mailbox instances.
   */
  final object Mailbox {
    private val params = Scope("mailbox")

    // Staging flags

    // The mailbox uses the stack to avoid allocations. This flag limits
    // the number of stack frames used by that optimization.
    val readMaxStackDepth = params(Usage.Staging, "readMaxStackDepth", 64)

    // Fiber mailboxes use fifo order, which has better performance for the
    // execution of future continuations and maintains the same behavior
    // as the default `LocalScheduler`
    val fiberMailboxFifo = params(Usage.Staging, "fiberMailboxFifo", true)

    // Worker mailboxes use lifo order so new fibers have priority over older
    // fibers, resulting in a behavior similar to Linux's O(1) scheduler.
    val workerMailboxFifo = params(Usage.Staging, "workerMailboxFifo", false)

    // Fiber mailboxes are restricted so they can't have tasks stolen after
    // they're read from the inbox. Setting this flag to false wouldn't change any
    // behavior since there isn't a mechanism that steals continuations from a fiber
    val fiberMailboxRestricted = params(Usage.Staging, "fiberMailboxRestricted", true)

    // Worker mailboxes are unrestricted so any tasks can be stolen, even after
    // they're read from the inbox.
    val workerMailboxRestricted = params(Usage.Staging, "workerMailboxRestricted", false)

    // Pads the atomic reference array of unrestricted mailboxes to avoid false
    // sharing. It's disabled by default because the initial tests show a
    // performance regression with this flag enabled, indicating that false
    // sharing isn't an issue
    val padUnrestrictedMailboxes = params(Usage.Staging, "padUnrestrictedMailboxes", false)
  }

  private[this] case class Scope(scope: String) {
    def apply[T](
      usage: Usage,
      name: String,
      default: T,
    )(
      implicit flaggable: Flaggable[T]
    ) = {
      val property = s"com.twitter.finagle.exp.fiber_scheduler.$scope.$name"
      val value = Option(System.getProperty(property))
        .map(flaggable.parse).getOrElse(default)
      val encodedValue: Long =
        value match {
          case v: Int => v.toLong
          case v: Duration => v.inMilliseconds
          case v: Double => (v * 1000).toLong
          case true => 1L
          case false => 0L
          case v =>
            Config.log.error(s"Can't encode config value for $usage property $property: $v")
            -1L
        }
      configStatsReceiver.counter(name).incr(encodedValue)
      value
    }
  }
}
