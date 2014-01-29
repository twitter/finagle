package com.twitter.finagle.service

import com.twitter.finagle.{
  SimpleFilter, Service, RequestTimeoutException, IndividualRequestTimeoutException}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Duration, Timer}

object TimeoutFilter {
  val TimeoutAnnotation = "finagle.timeout"
}

/**
 * A [[com.twitter.finagle.Filter]] that applies a global timeout to requests.
 *
 * @param timeout the timeout to apply to requests
 * @param exception an exception object to return in cases of timeout exceedance
 * @param timer a `Timer` object used to track elapsed time
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
