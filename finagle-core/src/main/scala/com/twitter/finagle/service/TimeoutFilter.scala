package com.twitter.finagle.service

import com.twitter.finagle._
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Duration, Timer}

object TimeoutFilter {
  val TimeoutAnnotation = "finagle.timeout"

  object RequestTimeout extends Stack.Role

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module.
   */
  case class Param(timeout: Duration)
  implicit object Param extends Stack.Param[Param] {
    val default = Param(Duration.Top)
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](RequestTimeout) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val TimeoutFilter.Param(timeout) = params[TimeoutFilter.Param]
        val param.Timer(timer) = params[param.Timer]
        if (!timeout.isFinite) next
        else {
          val exc = new IndividualRequestTimeoutException(timeout)
          new TimeoutFilter(timeout, exc, timer) andThen next
        }
      }
    }
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
