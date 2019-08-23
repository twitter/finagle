package com.twitter.finagle.liveness

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Status
import com.twitter.finagle.stats.{
  MultiCategorizingExceptionStatsHandler,
  NullStatsReceiver,
  StatsReceiver,
  Verbosity
}
import com.twitter.finagle.util._
import com.twitter.util._

/**
 * The threshold failure detector uses session pings to gauge the health
 * of a peer. It sends ping messages periodically and records their RTTs.
 *
 * The session is marked [[Status.Open]] until it starts slowing or failing.
 *
 * If no ping responses has been received within `closeThreshold`, the
 * session is marked as [[Status.Closed]].
 *
 * This scheme is pretty conservative, and is primarily intended to detect a bad
 * connection. Other modules will address issues such as slow host detection.
 *
 * A concern is the ''cost'' of pinging. In large-scale settings, where
 * each server may have thousands of clients, and each client thousands of
 * sessions, the cost of sending even trivially small messages can be significant.
 * In degenerate cases, pinging could consume the majority of a server process's
 * time. Thus our defaults are conservative, even if it means our detection latency
 * increases.
 */
private class ThresholdFailureDetector(
  ping: () => Future[Unit],
  minPeriod: Duration = 5.seconds,
  closeTimeout: Duration = 4.seconds,
  nanoTime: () => Long = System.nanoTime _,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  implicit val timer: Timer = DefaultTimer)
    extends FailureDetector {
  private[this] val failureHandler = new MultiCategorizingExceptionStatsHandler()
  private[this] val pingLatencyStat = statsReceiver.stat(Verbosity.Debug, "ping_latency_us")
  private[this] val closeCounter = statsReceiver.counter("close")
  private[this] val pingCounter = statsReceiver.counter("ping")

  // The timestamp of the last ping, in nanoseconds.
  @volatile private[this] var timestampNs: Long = 0L

  // start as open
  @volatile private[this] var state: Status = Status.Open

  def status: Status = state

  private[this] val _onClose = new Promise[Unit]

  def onClose: Future[Unit] = _onClose

  private[this] def markClosed(): Unit = {
    closeCounter.incr()
    state = Status.Closed
    _onClose.setDone()
  }

  private[this] def loop(): Future[Unit] = {
    pingCounter.incr()
    timestampNs = nanoTime()
    val p = ping()

    p.within(closeTimeout).transform {
      case Return(_) =>
        val rtt = nanoTime() - timestampNs
        pingLatencyStat.add(rtt.toFloat / 1000)
        Future.sleep(minPeriod - rtt.nanoseconds) before loop()
      case Throw(ex) =>
        failureHandler.record(statsReceiver, ex)
        markClosed()
        Future.exception(ex)
    }
  }

  // Note that we assume that the underlying ping() mechanism will
  // simply fail when the accrual detector is no longer required. If
  // ping can fail in other ways, and closeTimeout is not defined,
  // we may fail to do accrual (and indeed be forever stuck).
  loop()
}
