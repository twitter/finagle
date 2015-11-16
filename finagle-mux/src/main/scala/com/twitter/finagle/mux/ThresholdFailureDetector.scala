package com.twitter.finagle.mux

import java.util.concurrent.atomic.AtomicReference

import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.finagle.stats.{MultiCategorizingExceptionStatsHandler, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util._
import com.twitter.util._

/**
 * The threshold failure detector uses session pings to gauge the health
 * of a peer. It sends ping messages periodically and records their RTTs.
 *
 * The session is marked [[Status.Busy]] until the first successful ping
 * response has been received.
 *
 * If a ping has been sent and has been outstanding for a time greater
 * than the threshold multiplied by max ping latency over a number of
 * observations, the session is marked as [[Status.Busy]]. It is marked
 * [[Status.Open]] when the ping message returns.
 *
 * If no ping responses has been received within `closeThreshold`, the
 * session is marked as [[Status.Closed]].
 *
 * This scheme is pretty conservative, but it requires fairly little a priori
 * knowledge: the parameters are used only to tune its sensitivity to
 * history and to bound its failure detection time. The time to detection
 * is bounded by the ping period plus the threshold multiplied by max ping
 * RTT over a history.
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
    threshold: Double = 2,
    windowSize: Int = 100,
    closeTimeout: Duration = Duration.Top,
    nanoTime: () => Long = System.nanoTime,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    implicit val timer: Timer = DefaultTimer.twitter)
  extends FailureDetector {
  require(windowSize > 0)
  private[this] val failureHandler = new MultiCategorizingExceptionStatsHandler()
  private[this] val pingLatencyStat = statsReceiver.stat("ping_latency_us")
  private[this] val closeCounter = statsReceiver.counter("close")
  private[this] val pingCounter = statsReceiver.counter("ping")
  private[this] val busyCounter = statsReceiver.counter("marked_busy")
  private[this] val revivalCounter = statsReceiver.counter("revivals")

  private[this] val maxPingNs: WindowedMax = new WindowedMax(windowSize)
  // The timestamp of the last ping, in nanoseconds.
  @volatile private[this] var timestampNs: Long = 0L

  // start as busy, and become open after receiving the first ping response
  private[this] val state: AtomicReference[Status] = new AtomicReference(Status.Busy)

  private[this] val onBusyTimeout: Throwable => Unit =
    x => x match {
      case _: TimeoutException => markBusy()
      case _ =>
    }

  def status: Status = state.get

  private[this] def markBusy(): Unit = {
    if (state.compareAndSet(Status.Open, Status.Busy)) {
      busyCounter.incr()
    }
  }

  private[this] def markOpen(): Unit = {
    if (state.compareAndSet(Status.Busy, Status.Open)) {
      revivalCounter.incr()
    }
  }

  private[this] def markClosed(): Unit = {
    closeCounter.incr()
    state.set(Status.Closed)
  }

  private[this] def loop(): Future[Unit] = {
    pingCounter.incr()
    timestampNs = nanoTime()
    val p = ping()

    val max = maxPingNs.get
    val busyTimeout = (threshold * max).toLong.nanoseconds

    p.within(busyTimeout).onFailure(onBusyTimeout)

    p.within(closeTimeout).transform {
      case Return(_) =>
        val rtt = nanoTime() - timestampNs
        pingLatencyStat.add(rtt.toFloat/1000)
        maxPingNs.add(rtt)
        markOpen()
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

/**
 * Maintains the maximum value over most recent `windowSize` elements of the stream, providing
 * amortized O(1) for insertion, and O(1) for fetching max.
 *
 * @param windowSize the size of the window to keep track of
 */
private[mux] class WindowedMax(windowSize: Int) {
  @volatile private[this] var currentMax: Long = Long.MinValue
  private[this] val buf: Array[Long] = Array.fill(windowSize)(Long.MinValue)
  private[this] var index: Int = 0

  // Amortized 0(1)
  def add(value: Long): Unit = synchronized {
    if (value > currentMax) {
      currentMax = value
    }

    val prev = buf(index)
    buf(index) = value
    index = (index + 1) % windowSize

    // We should recalculate currentMax if it was evicted from the window.
    if (prev == currentMax && currentMax != value) {
      var i = 0
      var nextMax = Long.MinValue
      while (i < windowSize) {
        val v = buf(i)
        nextMax = math.max(nextMax, v)
        i = i + 1
      }
      currentMax = nextMax
    }
  }

  // O(1)
  def get: Long = currentMax
}