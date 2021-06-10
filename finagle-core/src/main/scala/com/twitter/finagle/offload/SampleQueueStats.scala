package com.twitter.finagle.offload

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{FuturePool, Stopwatch, Time, Timer}

// Helper that adds some instrumentation to a `FuturePool`
private final class SampleQueueStats(
  pool: FuturePool,
  stats: StatsReceiver,
  timer: Timer)
    extends (() => Unit) {

  private val delayMs = stats.stat("delay_ms")
  private val pendingTasks = stats.stat("pending_tasks")

  // No need to volatile or synchronize this to ensure safe publishing as there is a
  // happens-before relationship between the thread submitting a task into the ExecutorService
  // (or FuturePool) and the task itself.
  private var submitted: Stopwatch.Elapsed = null

  private object task extends (() => Unit) {
    def apply(): Unit = {
      val delay = submitted()
      delayMs.add(delay.inMilliseconds)
      pendingTasks.add(pool.numPendingTasks)

      val nextAt = Time.now + statsSampleInterval() - delay
      // NOTE: if the delay happened to be longer than the sampling interval, the nextAt would be
      // negative. Scheduling a task under a negative time would force the Timer to treat it as
      // "run now". Thus the offloading delay is sampled at either 'sampleInterval' or 'delay',
      // whichever is longer.
      timer.schedule(nextAt)(SampleQueueStats.this())
    }
  }

  def apply(): Unit = {
    submitted = Stopwatch.start()
    pool(task())
  }
}
