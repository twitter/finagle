package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.util.JavaTimer
import com.twitter.util.Timer
import com.twitter.finagle.exp.fiber_scheduler.util.NextPowerOfTwo
import java.util.Arrays

/**
 * Estimates the throughput of workers using a rolling window.
 * A global estimate of the mean is maintained and assigned to
 * new workers while their windows still aren't full.
 *
 * The throughput estimation doesn't reflect the worker's capacity
 * directly since it's based on the observed throughput, which could
 * be low due to low incoming load. This limitation is beneficial
 * since it allows the workers to reject large and sudden increases in
 * load and gives an opportunity for the optimizer to react to the change
 * by creating new workers if necessary.
 */
private[fiber_scheduler] abstract class WorkerThroughput {
  def perMs(): Double
  def stop(): Unit
}

private[fiber_scheduler] final object WorkerThroughput {

  // Use a dedicated timer thread to make reliable measurements
  implicit val timer: Timer = new JavaTimer(isDaemon = true, Some("fiber/throughput/timer"))

  // mechanism to measure a low-resolution mean of the throughput of all workers
  private[this] val meanResolutionMs = 0.001
  private[this] var meanEstimateMs = meanResolutionMs

  private[this] val window = NextPowerOfTwo(Config.Scheduling.workerThroughputWindow)
  private[this] val interval = Config.Scheduling.workerThroughputInterval
  private[this] val intervalMs = Config.Scheduling.workerThroughputInterval.inMillis
  private[this] val windowMs = intervalMs * window
  private[this] val mask = window - 1

  def meanEstimateMs(): Double = meanEstimateMs

  def apply(executions: => Long)(implicit timer: Timer): WorkerThroughput = {
    new WorkerThroughput {

      var last = 0D
      var index = 0
      @volatile var throughput = meanEstimateMs
      val measurements = new Array[Double](window)
      Arrays.fill(measurements, meanEstimateMs)

      val task =
        timer.schedule(interval) {
          val c = executions
          measurements(index) = c - last
          last = c

          val sum = measurements.sum
          throughput = sum / windowMs

          // update the mean estimate
          if (throughput > meanEstimateMs) {
            meanEstimateMs += meanResolutionMs
          } else if (throughput < meanEstimateMs) {
            meanEstimateMs -= meanResolutionMs
          }
          index = (index + 1) & mask
        }

      def perMs(): Double = throughput
      def stop(): Unit = task.cancel()
    }
  }
}
