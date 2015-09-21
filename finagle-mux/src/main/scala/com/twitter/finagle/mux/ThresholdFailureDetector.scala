package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.finagle.stats.{MultiCategorizingExceptionStatsHandler, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util._
import com.twitter.util._
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicReference

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
 * If `closeThreshold` is positive and no ping responses has been received
 * during a window of `closeThreshold` * max ping latency, then the `close`
 * function is called.
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
    close: () => Future[Unit],
    minPeriod: Duration = 5.seconds,
    threshold: Double = 2,
    windowSize: Int = 100,
    closeThreshold: Int = -1,
    nanoTime: () => Long = System.nanoTime,
    darkMode: Boolean = false,
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


  def status: Status =
    if (darkMode) Status.Open
    else state.get

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

  private[this] def loop(): Future[Unit] = {
    pingCounter.incr()
    timestampNs = nanoTime()
    val p = ping()

    val max = maxPingNs.get
    val busyTimeout = (threshold * max).toLong.nanoseconds
    val closeTimeout: Duration =
      if (closeThreshold <= 0) Duration.Top else {
        if (max == Long.MinValue) minPeriod * 12 // Arbitrary timeout used for the first ping
        else (closeThreshold * max).nanoseconds
      }

    p.within(busyTimeout).onFailure {
      case _: TimeoutException => markBusy()
      case _ =>
    }

    p.within(closeTimeout).onFailure {
      case _: TimeoutException =>
        closeCounter.incr()
        close()
      case _ =>
    }

    p.transform {
      case Return(_) =>
        val rtt = nanoTime() - timestampNs
        pingLatencyStat.add(rtt.toFloat/1000)
        maxPingNs.add(rtt)
        markOpen()
        Future.sleep(minPeriod - rtt.nanoseconds) before loop()
      case Throw(ex) =>
        markBusy()
        failureHandler.record(statsReceiver, ex)
        Future.exception(ex)
    }
  }

  // Note that we assume that the underlying ping() mechanism will
  // simply fail when the accrual detector is no longer required. If
  // ping can fail in other ways, we may fail to do accrual (and indeed
  // be forever stuck).
  loop()
}

private object WindowedMax {
  private case class AgedLong(v: Long, age: Int)
}

/**
 * Maintains maximum value over a history.
 *
 * Build based on Deque. New elements are inserted from the tail.
 * Smaller elements from the tail (if any) are removed before a new
 * insertion. This guarantees the max element can always be fetched
 * from the head of deque.
 *
 * Each element is inserted and removed once from deque, providing
 * O(n) for insertion, and O(1) for fetching max.
 *
 * @param window the number of history values to keep
 */
private class WindowedMax(window: Int) {
  import WindowedMax._
  require(window > 0)

  private[this] val values: ArrayDeque[AgedLong] = new ArrayDeque[AgedLong](window)
  private[this] var currAge: Int = -1

  def add(v: Long): Unit = synchronized {
    currAge += 1
    // remove aged element from head
    if (!values.isEmpty && currAge - values.peekFirst.age >= window) {
      values.pollFirst()
    }

    while (!values.isEmpty && v > values.getLast.v) {
      values.pollLast()
    }

    values.addLast(AgedLong(v, currAge))
  }

  def get: Long = synchronized {
    if (values.isEmpty) Long.MinValue
    else values.peekFirst.v
  }
}