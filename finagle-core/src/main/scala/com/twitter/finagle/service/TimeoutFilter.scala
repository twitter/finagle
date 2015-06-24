package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.client.LatencyCompensation
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Duration, Timer, Time}

object TimeoutFilter {
  val TimeoutAnnotation = "finagle.timeout"

  val role = new Stack.Role("RequestTimeout")

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module.
   */
  case class Param(timeout: Duration) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(Duration.Top))
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
      val role = TimeoutFilter.role
      val description = "Apply a timeout-derived deadline to requests; adjust existing deadlines."
      def make(_param: Param, _timer: param.Timer,
          _compensation: LatencyCompensation.Compensation,
          next: ServiceFactory[Req, Rep]) = {
        val Param(timeout) = _param
        val param.Timer(timer) = _timer
        val LatencyCompensation.Compensation(compensation) = _compensation

        val exc = new IndividualRequestTimeoutException(timeout)
        val filter = new TimeoutFilter[Req, Rep](timeout + compensation, exc, timer)
        filter andThen next
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
      val role = TimeoutFilter.role
      val description = "Apply a timeout-derived deadline to requests; adjust existing deadlines."
      def make(_param: Param, _timer: param.Timer, next: ServiceFactory[Req, Rep]) = {
        val Param(timeout) = _param
        val param.Timer(timer) = _timer
        if (!timeout.isFinite) next else {
          val exc = new IndividualRequestTimeoutException(timeout)
          val filter = new TimeoutFilter[Req, Rep](timeout, exc, timer)
          filter andThen next
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
    val timeoutDeadline = Deadline.ofTimeout(timeout)

    // If there's a current deadline, we combine it with the one derived
    // from our timeout.
    val deadline = Contexts.broadcast.get(Deadline) match {
      case Some(current) =>
        Deadline.combined(timeoutDeadline, current)
      case None => timeoutDeadline
    }

    Contexts.broadcast.let(Deadline, deadline) {
      val res = service(request)
      res.within(timer, timeout) rescue {
        case exc: java.util.concurrent.TimeoutException =>
          res.raise(exc)
          Trace.record(TimeoutFilter.TimeoutAnnotation)
          Future.exception(exception)
      }
    }
  }
}