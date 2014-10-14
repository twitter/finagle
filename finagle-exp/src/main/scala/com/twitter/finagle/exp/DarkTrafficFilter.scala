package com.twitter.finagle.exp

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.logging.Logger
import com.twitter.util.Future

/**
 * Forwards dark traffic to the given service when the given function returns true for a request.
 * @param darkService Service to take dark traffic
 * @param enableSampling if function returns true, the request will forward
 * @param statsReceiver keeps stats for requests forwarded, skipped and failed.
 */
class DarkTrafficFilter[Req, Rep](
    darkService: Service[Req, Rep],
    enableSampling: Req => Boolean,
    statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep] {

  private[this] val scopedStatsReceiver      = statsReceiver.scope("darkTrafficFilter")
  private[this] val requestsForwardedCounter = scopedStatsReceiver.counter("forwarded")
  private[this] val requestsSkippedCounter   = scopedStatsReceiver.counter("skipped")
  private[this] val failedCounter            = scopedStatsReceiver.counter("failed")
  private[this] val log = Logger.get("DarkTrafficFilter")

  override def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val rep = service(request)
    darkRequest(request, service)
    rep
  }

  private[this] def darkRequest(request: Req, service: Service[Req, Rep]): Unit = {
    if (enableSampling(request)) {
      requestsForwardedCounter.incr()
      darkService(request).onFailure { t: Throwable =>
        // This may not count if you're using a one-way service
        failedCounter.incr()

        log.error(t, t.getMessage)
      }
    } else {
      requestsSkippedCounter.incr()
    }
  }
}
