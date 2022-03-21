package com.twitter.finagle.util

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.SlowProbeProxyTimer
import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.util.Timer
import scala.collection.JavaConverters._

/**
 * An implementation of `SlowProbeProxyTimer` which keeps a `Counter` for tasks
 * that have exceeded the specified runtime and attempts to log the stack traces
 * of all executing threads if a slow task is observed to be executing.
 *
 * @note The counter is exported under `timer/slow` in the provided `StatsReceiver`.
 */
private final class LoggingSlowProbeProxyTimer(
  underlying: Timer,
  statsReceiver: StatsReceiver,
  maxRuntime: Duration,
  maxLogFrequency: Duration)
    extends SlowProbeProxyTimer(maxRuntime) {

  private[this] val log = Logger.get
  private[this] val slow = statsReceiver.counter("timer", "slow")

  // thread-safety provided by synchronization on `this`
  private[this] var lastLog: Time = Time.Bottom

  protected def self: Timer = underlying

  protected def slowTaskCompleted(elapsed: Duration): Unit = slow.incr()

  protected def slowTaskExecuting(elapsed: Duration): Unit =
    if (shouldLog()) {
      val initialLine = s"Timer task has been running for more than $maxRuntime ($elapsed), " +
        "current stacktraces follow.\n"

      val traces = Thread.getAllStackTraces.asScala.map {
        case (thread, stack) =>
          "Slow Timer task thread dump. Thread id=" +
            s"${thread.getId} '${thread.getName}': ${stack.mkString("\n\t", "\n\t", "")}"
      }

      log.warning(traces.mkString(initialLine, "\n", ""))
    }

  private[this] def shouldLog(): Boolean = {
    val currentTime = Time.now
    synchronized {
      if (currentTime - lastLog <= maxLogFrequency) false
      else {
        lastLog = currentTime
        true
      }
    }
  }
}
