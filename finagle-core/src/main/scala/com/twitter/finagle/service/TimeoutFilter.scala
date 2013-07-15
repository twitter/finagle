package com.twitter.finagle.service

import com.twitter.finagle.{
  SimpleFilter, Service, RequestTimeoutException, IndividualRequestTimeoutException}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Duration, Timer}

/**
 * A filter to apply a global timeout to the request. This allows,
 * e.g., for a server to apply a global request timeout.
 */
class TimeoutFilter[Req, Rep](
    timeout: Duration,
    exception: RequestTimeoutException,
    timer: Timer)
    extends SimpleFilter[Req, Rep] {
  def this(timeout: Duration, timer: Timer) =
    this(timeout, new IndividualRequestTimeoutException(timeout), timer)

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val res = service(request)

    res.within(timer, timeout) rescue {
      case exc: java.util.concurrent.TimeoutException =>
        res.raise(exc)
        Trace.record(TimeoutFilter.TimeoutAnnotation)
        Future.exception(exception)
    }
  }
}

object TimeoutFilter {
  val TimeoutAnnotation = "finagle.timeout"
}
