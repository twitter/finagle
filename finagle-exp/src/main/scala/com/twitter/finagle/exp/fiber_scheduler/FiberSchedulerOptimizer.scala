package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.util.Promise
import com.twitter.finagle.exp.fiber_scheduler.util.Cgroup
import com.twitter.finagle.exp.fiber_scheduler.util.NextPowerOfTwo
import com.twitter.finagle.exp.fiber_scheduler.util.Optimizer
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.LongAdder

/**
 * Creates an `Optimizer` with the necessary configuration to support a `FiberScheduler`.
 */
object FiberSchedulerOptimizer {
  def apply(
    minThreads: Int,
    maxThreads: Int,
    startWorker: () => Promise[Unit],
    stopWorker: () => Promise[Unit],
    pool: ThreadPoolExecutor,
    completions: LongAdder,
    rejections: LongAdder,
    workers: CopyOnWriteArrayList[Worker]
  ): Optimizer = {
    import Optimizer._
    import Config.Optimizer._
    val cgroup = new Cgroup()

    // Memory cliff to address potential issues with container OOM
    // due to the use of off-heap memory for the thread stacks
    val memoryCliff =
      Limit
        .ifReaches("memory_usage_percent", memoryCliffMaxUsagePercent) {
          cgroup.memoryUsagePercent()
        }.withMaxTries(memoryCliffMaxTries, memoryCliffMaxTriesExpiration)
        .withCleanup {
          // triggers inactive threads expiration
          pool.allowCoreThreadTimeOut(true)
          pool.allowCoreThreadTimeOut(false)
        }

    // Uses cgroup files to detect CPU throttling and limit the number of
    // threads accordingly.
    val cpuThrottlingCliff =
      Limit
        .ifIncreases("cpu_nr_throttled") {
          cgroup.cpuNrThrottled()
        }.withMaxTries(cpuThrottlingCliffMaxTries, cpuThrottlingCliffMaxTriesExpiration)

    // Avoids reducing the number of threads if the scheduler is rejecting tasks.
    val rejectionsValley =
      Limit
        .ifIncreases("rejections")(rejections.sum())
        .withMaxTries(rejectionsValleyMaxTries, rejectionsValleyMaxTriesExpiration)

    // Detects workers running blocking tasks and avoids reducing the number of
    // threads if the percentage of blocked workers are above the threshold.
    val blockedWorkersValley =
      Limit
        .ifReaches("bocked_workers", blockedWorkersValleyMaxPercent) {
          var blocked = 0
          workers.forEach { w =>
            if (w.isBlocked)
              blocked += 1
          }
          (blocked.toDouble / workers.size()) * 100
        }.withMaxTries(blockedWorkersValleyMaxTries, blockedWorkersValleyMaxTriesExpiration)

    // Detects if the scheduler is mostly idle. The optimizer doesn't make adapt
    // decisions if the scheduler is is considered idle.
    val isIdle = () => {
      var active = 0D
      workers.forEach { w =>
        if (w.load() > 0)
          active += 1
      }
      (active / workers.size) * 100 < idleThresholdPercent
    }

    Optimizer(
      score = Score.delta(completions.sum()),
      cliffLimit = cpuThrottlingCliff.andThen(memoryCliff),
      valleyLimit = rejectionsValley.andThen(blockedWorkersValley),
      cliffExpiration = cliffExpiration,
      valleyExpiration = valleyExpiration,
      max = maxThreads,
      min = minThreads,
      get = () => workers.size(),
      up = startWorker,
      down = stopWorker,
      isIdle,
      adaptPeriod = NextPowerOfTwo(optimizerAdaptPeriod),
      wavePeriod = NextPowerOfTwo(optimizerWavePeriod),
      cycleInterval = {
        val p = cgroup.cpuPeriod
        if (p.isZero) {
          optimizerDefaultCycle
        } else {
          p
        }
      }
    )
  }
}
