package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.client.LatencyCompensation
import com.twitter.finagle.context.{Contexts, Deadline}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Duration, Future, Timer}

object TimeoutFilter {
  val TimeoutAnnotation: String = "finagle.timeout"

  /**
   * Used for a per request timeout.
   */
  val role: Stack.Role = Stack.Role("RequestTimeout")

  /**
   * Used for a total timeout for requests, including retries when applicable.
   */
  val totalTimeoutRole: Stack.Role = Stack.Role("Total Timeout")

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module.
   */
  case class Param(timeout: Duration) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param: Stack.Param[TimeoutFilter.Param] =
      Stack.Param(Param(Duration.Top))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module when used for
   * a total timeout of a logical request, including retries.
   */
  private[finagle] case class TotalTimeout(timeout: Duration) {
    def mk(): (TotalTimeout, Stack.Param[Param]) =
      (this, Param.param)
  }
  private[finagle] object TotalTimeout {
    implicit val param: Stack.Param[TotalTimeout] =
      Stack.Param(TotalTimeout(Duration.Top))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]]
   * for use in clients.
   */
  def clientModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[
        TimeoutFilter.Param,
        param.Timer,
        LatencyCompensation.Compensation,
        ServiceFactory[Req, Rep]] {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a timeout-derived deadline to requests; adjust existing deadlines."

      def make(
        _param: Param,
        _timer: param.Timer,
        _compensation: LatencyCompensation.Compensation,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val timeout = _param.timeout + _compensation.howlong

        if (!timeout.isFinite || timeout <= Duration.Zero) {
          next
        } else {
          val filter = new TimeoutFilter[Req, Rep](
            () => timeout,
            timeout => new IndividualRequestTimeoutException(timeout),
            _timer.timer)
          filter.andThen(next)
        }
      }
    }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]]
   * for use in servers.
   */
  def serverModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[
        TimeoutFilter.Param,
        param.Timer,
        ServiceFactory[Req, Rep]] {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a timeout-derived deadline to requests; adjust existing deadlines."
      def make(
        _param: Param,
        _timer: param.Timer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val Param(timeout) = _param
        val param.Timer(timer) = _timer
        if (!timeout.isFinite || timeout <= Duration.Zero) next else {
          val exc = new IndividualRequestTimeoutException(timeout)
          val filter = new TimeoutFilter[Req, Rep](timeout, exc, timer)
          filter.andThen(next)
        }
      }
    }

  def typeAgnostic(
    timeout: Duration,
    exception: RequestTimeoutException,
    timer: Timer
  ): TypeAgnostic = new TypeAgnostic {
    override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new TimeoutFilter[Req, Rep](timeout, exception, timer)
  }
}

/**
 * A [[com.twitter.finagle.Filter]] that applies a timeout to requests.
 *
 * If the response is not satisfied within the `timeout`,
 * the [[Future]] will be interrupted via [[Future.raise]].
 *
 * @param timeoutFn the timeout to apply to requests
 * @param exceptionFn an exception object to return in cases of timeout exceedance
 * @param timer a `Timer` object used to track elapsed time
 *
 * @see The sections on
 *      [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration clients]]
 *      and [[https://twitter.github.io/finagle/guide/Servers.html#request-timeout servers]]
 *      in the user guide for more details.
 */
class TimeoutFilter[Req, Rep](
    timeoutFn: () => Duration,
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer)
  extends SimpleFilter[Req, Rep] {

  def this(timeout: Duration, exception: RequestTimeoutException, timer: Timer) =
    this(() => timeout, _ => exception, timer)

  def this(timeout: Duration, timer: Timer) =
    this(timeout, new IndividualRequestTimeoutException(timeout), timer)

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val timeout = timeoutFn()
    val timeoutDeadline = Deadline.ofTimeout(timeout)

    // If there's a current deadline, we combine it with the one derived
    // from our timeout.
    val deadline = Deadline.current match {
      case Some(current) => Deadline.combined(timeoutDeadline, current)
      case None => timeoutDeadline
    }

    Contexts.broadcast.let(Deadline, deadline) {
      val res = service(request)
      if (!timeout.isFinite) {
        res
      } else {
        res.within(timer, timeout).rescue {
          case exc: java.util.concurrent.TimeoutException =>
            res.raise(exc)
            Trace.record(TimeoutFilter.TimeoutAnnotation)
            Future.exception(exceptionFn(timeout))
        }
      }
    }
  }

}
