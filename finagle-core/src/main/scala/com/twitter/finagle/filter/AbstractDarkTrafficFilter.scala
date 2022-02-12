package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.filter.DarkTrafficFilter.DarkRequestAnnotation
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.ForwardAnnotation
import com.twitter.util.Future
import com.twitter.util.Promise
import scala.util.control.NonFatal

object AbstractDarkTrafficFilter {
  val StatsScope: String = "dark_traffic_filter"
}

trait AbstractDarkTrafficFilter {
  import AbstractDarkTrafficFilter._

  /* Abstract Members */
  protected val statsReceiver: StatsReceiver
  protected def handleFailedInvocation[Req](request: Req, t: Throwable): Unit

  /* Private  */
  private[this] val scopedStatsReceiver = statsReceiver.scope(StatsScope)
  private[this] val requestsForwardedCounter = scopedStatsReceiver.counter("forwarded")
  private[this] val requestsSkippedCounter = scopedStatsReceiver.counter("skipped")
  private[this] val failedCounter = scopedStatsReceiver.counter("failed")

  private[this] def handleFailure[Req](request: Req, t: Throwable): Unit = {
    // This may not count if you're using a one-way service
    failedCounter.incr()
    handleFailedInvocation(request, t)
  }

  /* Protected */

  // We separate the argument list to allow the compiler
  // to provide better type hints for the second list which contains
  // the shouldInvoke function which allows [Req] to be a higher-kinded type.
  protected def serviceConcurrently[Req, Rep](
    service: Service[Req, Rep],
    request: Req
  )(
    shouldInvoke: Boolean,
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

  protected def sendDarkRequest[Req](
    request: Req
  )(
    shouldInvoke: Boolean,
    invokeDarkService: Req => Future[_]
  ): Future[_] = {
    ForwardAnnotation.let(DarkRequestAnnotation) {
      if (shouldInvoke) {
        requestsForwardedCounter.incr()
        invokeDarkService(request)
          .onFailure(handleFailure(request, _))
          .unit
      } else {
        requestsSkippedCounter.incr()
        Future.Done
      }
    }
  }
}
