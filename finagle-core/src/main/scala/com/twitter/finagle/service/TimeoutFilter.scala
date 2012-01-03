package com.twitter.finagle.service

import com.twitter.finagle.{
  Filter, Service, RequestTimeoutException, IndividualRequestTimeoutException}
import com.twitter.finagle.util.Timer
import com.twitter.finagle.tracing.Trace
import com.twitter.util
import com.twitter.util.{Future, Duration}

/**
 * A filter to apply a global timeout to the request. This allows,
 * e.g., for a server to apply a global request timeout.
 */
class TimeoutFilter[Req, Rep](
    timeout: Duration,
    exception: RequestTimeoutException,
    timer: util.Timer = Timer.default)
    extends Filter[Req, Rep, Req, Rep] {
  def this(timeout: Duration) = this(timeout, new IndividualRequestTimeoutException(timeout))

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val res = service(request)

    res.within(timer, timeout) rescue {
      case _: java.util.concurrent.TimeoutException =>
        res.cancel()
        Trace.record("finagle.timeout")
        Future.exception(exception)
    }
  }
}
