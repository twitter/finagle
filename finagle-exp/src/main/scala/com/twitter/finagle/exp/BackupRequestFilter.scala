package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.{Failure, BackupRequestLost, Service, SimpleFilter}
import com.twitter.util.{Duration, Future, Return, Throw, Timer, Stopwatch}
import java.util.concurrent.atomic.AtomicInteger

object BackupRequestFilter {
  /**
   * Throwable used when the backup request timer is aborted.
   */
  private[exp] val cancelEx = Failure("backup countdown cancelled")

  /**
   * Default for calculating now in milliseconds.
   */
  val DefaultNowMs: () => Long = Stopwatch.systemMillis

  /**
   * Default value for how many requests trigger a recalculation of the cutoff.
   */
  val DefaultRecalcWindow = 100

  private[exp] def defaultError(clipDuration: Duration): Double =
    if (clipDuration.inMillis < 100) 0.0 else 0.05

}

/**
 * Issue a backup request after a threshold time has elapsed, useful
 * for curtailing tail latencies in distributed systems. See [1] for details.
 *
 * Several stats are exported through the given stats receiver exposing
 * the filter's operating behavior:
 *
 *  - counter `timeouts` indicates the number of original requests
 *    that timed out and subsequently caused a backup request to
 *    be issued;
 *  - counter `won` indicates the number of original requests that won,
 *    including those that didn't actually race with a backup;
 *  - counter `lost` indicates the number of original requests that lost
 *    a race with their corresponding backup request, and finally
 *  - gauge `cutoff_ms` indicates the current backup request cutoff value
 *    in milliseconds.
 *
 * [1]: Dean, J. and Barroso, L.A. (2013), The Tail at Scale,
 * Communications of the ACM, Vol. 56 No. 2, Pages 74-80,
 * http://cacm.acm.org/magazines/2013/2/160173-the-tail-at-scale/fulltext
 *
 * @param quantile The response latency quantile at which to issue
 * a backup request. Must be between 0 and 100, exclusive.
 *
 * @param clipDuration the range of expected durations, values above this
 * range are clipped. Must be less than 1 hour.
 *
 * @param timer The timer to be used in scheduling backup requests.
 *
 * @param history how long to remember data points when calculating
 * quantiles.
 *
 * @param recalculateWindow this number of requests will trigger
 * a recalculation of what the cutoff time is for the given `quantile`.
 * Value must be greater than 0. A value of 1 will cause the cutoff to
 * be calculated for every request.
 *
 * @param quantileError the allowed error percent for calculating quantiles.
 * If `0.0`, then the granularity will be one unit of `clipDuration`.
 * If `(0.0, 1.0]` then it is used as a percentage of `clipDuration`.
 * Values greater than `1.0` or less than `0.0` are invalid.
 * Using a small value will give more accurate quantile computations
 * with the tradeoff of more memory used.
 *
 * @note Care must be taken to ensure that application of this filter
 * preserves semantics since a request may be issued twice (ie. they
 * are idempotent).
 *
 * @note A filter alone cannot guarantee that a request is dispatched
 * onto a different endpoint from the original. Eventually, this
 * should be implemented as a sort of queueing policy.
 */
class BackupRequestFilter[Req, Rep] private[exp](
    quantile: Int,
    clipDuration: Duration,
    timer: Timer,
    statsReceiver: StatsReceiver,
    history: Duration,
    nowMs: () => Long,
    recalculateWindow: Int,
    quantileError: Double)
  extends SimpleFilter[Req, Rep] {

  def this(
    quantile: Int,
    clipDuration: Duration,
    timer: Timer,
    statsReceiver: StatsReceiver,
    history: Duration
  ) = this(
    quantile,
    clipDuration,
    timer,
    statsReceiver,
    history,
    BackupRequestFilter.DefaultNowMs,
    BackupRequestFilter.DefaultRecalcWindow,
    BackupRequestFilter.defaultError(clipDuration)
  )

  require(quantile > 0 && quantile < 100)
  require(clipDuration < 1.hour)
  require(recalculateWindow >= 1)

  private[this] val histo = new LatencyHistogram(
    clipDuration.inMilliseconds,
    quantileError,
    history.inMilliseconds,
    LatencyHistogram.DefaultSlices,
    nowMs)

  @volatile
  private[this] var cachedCutoffMs = 0L
  private[this] val count = new AtomicInteger()

  /**
   * Returns the cutoff, in milliseconds, when a backup request
   * should be sent.
   *
   * If `0` is returned, no backup request will be sent.
   *
   * (package protected for testing)
   */
  private[exp] def cutoffMs(): Long = {
    if (count.incrementAndGet() % recalculateWindow == 0)
      cachedCutoffMs = histo.quantile(quantile)
    cachedCutoffMs
  }

  private[this] val timeouts = statsReceiver.counter("timeouts")
  private[this] val won = statsReceiver.counter("won")
  private[this] val lost = statsReceiver.counter("lost")
  private[this] val cutoffGauge = statsReceiver.addGauge("cutoff_ms") {
    cachedCutoffMs
  }

  private[this] def record(f: Future[Rep], successCounter: Counter): Future[Rep] = {
    val start = nowMs()
    f.onSuccess { _ =>
      successCounter.incr()
      histo.add(nowMs() - start)
    }
  }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val orig = record(service(req), won)
    val howLong = cutoffMs()

    if (howLong == 0)
      return orig

    val backupCountdown = Future.sleep(Duration.fromMilliseconds(howLong))(timer)
    val backupTriggers = Array(orig, backupCountdown)

    Future.selectIndex(backupTriggers).flatMap { firstIndex =>
      val first = backupTriggers(firstIndex)
      if (first eq orig) {
        // If the orig request is successful before the backup timer has fired,
        // return the response. At this point we can can cancel the backup countdown
        // because it has either fired or we will dispatch a backup immediately.
        backupCountdown.raise(BackupRequestFilter.cancelEx)
        orig.transform {
          case r @ Return(v) => Future.const(r)
          case Throw(_) => record(service(req), lost)
        }
      } else {
        // If we've waited long enough to fire the backup normally, do so and
        // pass on the first successful result we get back.
        timeouts.incr()
        val backup = record(service(req), lost)
        val reps = Array(orig, backup)
        Future.selectIndex(reps).flatMap { firstIndex =>
          val first = reps(firstIndex)
          val other = reps((firstIndex+1)%2)
          first.transform {
            case r @ Return(v) =>
              if (first eq backup) orig.raise(BackupRequestLost)
              Future.const(r)
            case Throw(_) =>
              other
          }
        }
      }
    }
  }

}
