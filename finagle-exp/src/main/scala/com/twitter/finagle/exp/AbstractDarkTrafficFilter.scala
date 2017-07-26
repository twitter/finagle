package com.twitter.finagle.exp

import com.twitter.finagle.Service
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Promise, Future}
import scala.util.control.NonFatal

trait AbstractDarkTrafficFilter {

  /* Abstract Members */
  protected val statsReceiver: StatsReceiver
  protected def handleFailedInvocation(t: Throwable): Unit

  /* Private  */
  private[this] val scopedStatsReceiver = statsReceiver.scope("dark_traffic_filter")
  private[this] val requestsForwardedCounter = scopedStatsReceiver.counter("forwarded")
  private[this] val requestsSkippedCounter = scopedStatsReceiver.counter("skipped")
  private[this] val failedCounter = scopedStatsReceiver.counter("failed")

  private[this] val handleFailure: (Throwable) => Unit = { t: Throwable =>
    // This may not count if you're using a one-way service
    failedCounter.incr()
    handleFailedInvocation(t)
  }

  /* Protected */

  // We separate the argument list to allow the compiler
  // to provide better type hints for the second list which contains
  // the shouldInvoke function which allows [Req] to be a higher-kinded type.
  protected def serviceConcurrently[Req, Rep](service: Service[Req, Rep], request: Req)(
    shouldInvoke: Req => Boolean,
    invokeDarkService: Req => Future[_]
  ): Future[Rep] = {

    val p = new Promise[Rep]
    val rep = service(request)
    val darkResponse = sendDarkRequest(request)(shouldInvoke, invokeDarkService)
    rep.proxyTo(p)

    p.setInterruptHandler {
      case NonFatal(t) =>
        rep.raise(t)
        darkResponse.raise(t)
    }
    p
  }

  protected def sendDarkRequest[Req](request: Req)(
    shouldInvoke: Req => Boolean,
    invokeDarkService: Req => Future[_]
  ): Future[_] = {

    if (shouldInvoke(request)) {
      requestsForwardedCounter.incr()
      invokeDarkService(request)
        .onFailure(handleFailure)
        .unit
    } else {
      requestsSkippedCounter.incr()
      Future.Done
    }
  }
}
