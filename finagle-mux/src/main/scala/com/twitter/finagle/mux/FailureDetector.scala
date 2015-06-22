package com.twitter.finagle.mux

import com.twitter.finagle.Status
import com.twitter.finagle.stats.{MultiCategorizingExceptionStatsHandler, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.{DefaultTimer, Ema}
import com.twitter.util._
import com.twitter.conversions.time._

/**
 * Failure detectors attempt to gauge the liveness of a peer,
 * usually by sending ping messages and evaluating response
 * times.
 */
private[mux] trait FailureDetector {
  def status: Status
}

/**
 * The null failure detector is the most conservative: it uses
 * no information, and always gauges the session to be
 * [[Status.Open]].
 */
private object NullFailureDetector extends FailureDetector {
  def status: Status = Status.Open
}

/**
 * The threshold failure detector uses session pings to gauge the health
 * of a peer. It sends ping messages periodically and records their RTTs.
 * These RTTs are averaged over a number of observations.
 *
 * The session is marked [[Status.Busy]] until the first successful ping
 * response has been received.
 *
 * If a ping has been sent and has been outstanding for a time greater
 * than the threshold multiplied by the current exponential moving
 * average, the session is marked as [[Status.Busy]]. It is marked
 * [[Status.Open]] when the ping message returns.
 *
 * If `closeThreshold` is positive and no ping responses has been received
 * during a window of `closeThreshold * ema.last`, then the `close`
 * function is called.
 *
 * This scheme is pretty conservative, but it requires fairly little a priori
 * knowledge: the parameters are used only to tune its sensitivity to
 * history and to bound its failure detection time. The time to detection
 * is bounded by the ping period plus the threshold multiplied by the
 * average ping RTT.
 */
private class ThresholdFailureDetector(
    ping: () => Future[Unit],
    close: () => Future[Unit],
    minPeriod: Duration = 100.milliseconds,
    threshold: Double = 2,
    windowSize: Int = 5,
    closeThreshold: Int = -1,
    nanoTime: () => Long = System.nanoTime,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    implicit val timer: Timer = DefaultTimer.twitter)
  extends FailureDetector {
  private[this] val ema = new Ema(windowSize)
  // The logical clock for EMA measurements. This is accessed
  // by one logical thread, and does not need to be protected.
  private[this] var emaTime = 0L

  // The timestamp of the last ping, in nanoseconds.
  @volatile private[this] var timestamp: Long = 0L

  private[this] val pingLatencyStat = statsReceiver.stat("ping_latency_us")
  private[this] val closeCounter = statsReceiver.counter("close")
  private[this] val pingCounter = statsReceiver.counter("ping")
  private[this] val busyCounter = statsReceiver.counter("marked_busy")
  private[this] val failureHandler = new MultiCategorizingExceptionStatsHandler()

  private[this] def loop(): Future[Unit] = {
    pingCounter.incr()
    timestamp = nanoTime()
    val hardTimeout =
      if (closeThreshold > 0)
        if (ema.last > 0) (closeThreshold * ema.last).toInt.nanoseconds
        else minPeriod * threshold * 16 // Arbitrary timeout used for the first ping
      else Duration.Top

    ping().within(hardTimeout) transform {
      case Return(_) =>
        val rtt = nanoTime() - timestamp
        pingLatencyStat.add(rtt.toFloat/1000)
        timestamp = 0L
        emaTime += 1
        ema.update(emaTime, rtt)
        Future.sleep(minPeriod - rtt.nanoseconds) before loop()
      case Throw(_: TimeoutException) =>
        closeCounter.incr()
        close()
      case Throw(ex) =>
        failureHandler.record(statsReceiver, ex)
        Future.exception(ex)
    }
  }

  /**
   * The number of nanoseconds pending on the current ping;
   * 0 if no ping is outstanding.
   */
  private[this] def pendingNs(): Long =
    timestamp match {
      case 0 => 0
      case t => nanoTime() - t
    }

  // Note that we assume that the underlying ping() mechanism will
  // simply fail when the accrual detector is no longer required. If
  // ping can fail in other ways, we may fail to do accrual (and indeed
  // be forever stuck).
  loop()

  def status: Status =
    if (ema.isEmpty || pendingNs() > threshold * ema.last) {
      busyCounter.incr()
      Status.Busy
    } else Status.Open
}
