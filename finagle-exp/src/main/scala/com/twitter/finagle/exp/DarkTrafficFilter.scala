package com.twitter.finagle.exp

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise}
import scala.util.control.NonFatal

/**
 * Forwards dark traffic to the given service when the given function returns true for a request.
  *
  * @param darkService Service to take dark traffic
 * @param enableSampling if function returns true, the request will forward
 * @param statsReceiver keeps stats for requests forwarded, skipped and failed.
 * @Param forwardAfterService forward the dark request after the service has processed the request
 *        instead of concurrently.
 */
class DarkTrafficFilter[Req, Rep](
    darkService: Service[Req, Rep],
    enableSampling: Req => Boolean,
    statsReceiver: StatsReceiver,
    forwardAfterService: Boolean)
  extends SimpleFilter[Req, Rep] {

  def this(
    darkService: Service[Req, Rep],
    enableSampling: Req => Boolean,
    statsReceiver: StatsReceiver
  ) = this(darkService, enableSampling, statsReceiver, false)

  private[this] val scopedStatsReceiver      = statsReceiver.scope("darkTrafficFilter")
  private[this] val requestsForwardedCounter = scopedStatsReceiver.counter("forwarded")
  private[this] val requestsSkippedCounter   = scopedStatsReceiver.counter("skipped")
  private[this] val failedCounter            = scopedStatsReceiver.counter("failed")
  private[this] val log = Logger.get("DarkTrafficFilter")

  override def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    if (forwardAfterService) {
      service(request).ensure {
        darkRequest(request)
      }
    } else {
      val p = new Promise[Rep]
      val rep = service(request)
      val darkRep = darkRequest(request)
      rep.proxyTo(p)

      p.setInterruptHandler { case NonFatal(t) =>
        rep.raise(t)
        darkRep.raise(t)
      }
      p
    }
  }

  private[this] def darkRequest(request: Req): Future[Unit] = {
    if (enableSampling(request)) {
      requestsForwardedCounter.incr()
      darkService(request).onFailure { t: Throwable =>
        // This may not count if you're using a one-way service
        failedCounter.incr()

        log.error(t, t.getMessage)
      }.unit
    } else {
      requestsSkippedCounter.incr()
      Future.Done
    }
  }
}
