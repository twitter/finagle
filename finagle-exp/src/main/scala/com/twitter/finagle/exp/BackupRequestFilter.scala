package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.{Service, SimpleFilter, NoStacktrace, BackupRequestLost}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Future, Return, Throw, Duration, Timer, Stopwatch}

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
 *  - guage `cutoff_ms` indicates the current backup request cutoff value
 *    in milliseconds.
 *
 * [1]: Dean, J. and Barroso, L.A. (2013), The Tail at Scale,
 * Communications of the ACM, Vol. 56 No. 2, Pages 74-80,
 * http://cacm.acm.org/magazines/2013/2/160173-the-tail-at-scale/fulltext
 *
 * @param quantile The response latency quantile at which to issue
 * a backup request.
 *
 * @param range the range of expected durations, values above this
 * range are clipped to the maximum.
 *
 * @param timer The timer to be used in scheduling backup requests.
 *
 * @note Care must be taken to ensure that application of this filter
 * preserves semantics since a request may be issued twice (ie. they
 * are idempotent).
 *
 * @note A filter alone cannot guarantee that a request is dispatched
 * onto a different endpoint from the original. Eventually, this
 * should be implemented as a sort of queueing policy.
 */
class BackupRequestFilter[Req, Rep](
    quantile: Int, 
    range: Duration, 
    timer: Timer, 
    statsReceiver: StatsReceiver,
    history: Duration,
    stopwatch: Stopwatch = Stopwatch
) extends SimpleFilter[Req, Rep] {
  require(quantile > 0 && quantile < 100)
  require(range < 1.hour)

  // Given that we read into the LongAdders as much as we write to
  // them, it's unclear how much their use is helping.
  private[this] val histo = new LatencyHistogram(range, history)
  private[this] def cutoff() = histo.quantile(quantile)

  private[this] val timeouts = statsReceiver.counter("timeouts")
  private[this] val won = statsReceiver.counter("won")
  private[this] val lost = statsReceiver.counter("lost")
  private[this] val cutoffGauge = 
    statsReceiver.addGauge("cutoff_ms") { cutoff().inMilliseconds.toFloat }

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = stopwatch.start()
    val howlong = cutoff()
    val backup = if (howlong == Duration.Zero) Future.never else {
      timer.doLater(howlong) {
        timeouts.incr()
        service(req)
      } flatten
    }

    val orig = service(req)

    Future.select(Seq(orig, backup)) flatMap {
      case (Return(res), Seq(other)) =>
        if (other eq orig) lost.incr() else {
          won.incr()
          // Currently we use only latency data only from the 
          // first request, since that is simplest.
          histo.add(elapsed())
        }

        other.raise(BackupRequestLost)
        Future.value(res)
      case (Throw(_), Seq(other)) => other
    }
  }
}
