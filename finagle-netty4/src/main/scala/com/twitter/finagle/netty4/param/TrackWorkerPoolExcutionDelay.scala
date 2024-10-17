package com.twitter.finagle.netty4.param

import com.twitter.finagle.Stack
import com.twitter.util.Duration

/**
 * Control for tracking execution delay, cpu time, and active sockets in the worker threads for a
 * listener. This is intended to be enabled for perf tracking, and may impact performance as it adds
 * tracking runnables to the event executors. Stats will be written to the stats receiver for the
 * listener under workerpool/deviation_ms. When thread dumping is enabled, all logging is done at
 * the warning level.
 *
 * @param enableTracking  If true enable thread tracking.
 * @param trackingTaskPeriod The fixed time scheduling window for the execution delay runnable.
 * @param threadDumpThreshold If > 0ms, enable stack dumping of threads when they have been delayed for
 *                            more than the threshold. Thresholds of  < 10ms will not work as
 *                            expected as the underlying executors do not use high resolution timers.
 */
case class TrackWorkerPool(
  enableTracking: Boolean,
  trackingTaskPeriod: Duration,
  threadDumpThreshold: Duration) {
  def mk(): (TrackWorkerPool, Stack.Param[TrackWorkerPool]) =
    (this, TrackWorkerPool.trackWorkerPoolParam)

}

object TrackWorkerPool {
  implicit val trackWorkerPoolParam: Stack.Param[TrackWorkerPool] =
    Stack.Param[TrackWorkerPool](
      TrackWorkerPool(
        false,
        Duration.fromMilliseconds(0),
        Duration.fromMilliseconds(0)
      )
    )

}
