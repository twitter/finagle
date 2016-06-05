package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.client.LatencyCompensation
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Future, Duration, Timer}

object TimeoutFilter {
  val TimeoutAnnotation: String = "finagle.timeout"

  val role: Stack.Role = new Stack.Role("RequestTimeout")

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
    new Stack.Module4[
        TimeoutFilter.Param,
        param.Timer,
        LatencyCompensation.Compensation,
        param.Stats,
        ServiceFactory[Req, Rep]] {
      val role = TimeoutFilter.role
      val description = "Apply a timeout-derived deadline to requests; adjust existing deadlines."

      def make(
        _param: Param,
        _timer: param.Timer,
        _compensation: LatencyCompensation.Compensation,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val timeout = _param.timeout + _compensation.howlong

        if (!timeout.isFinite || timeout <= Duration.Zero) {
          next
        } else {
          val param.Timer(timer) = _timer
          val exc = new IndividualRequestTimeoutException(timeout)
          val param.Stats(stats) = _stats
          val filter = new TimeoutFilter[Req, Rep](
            timeout, exc, timer, stats.scope("timeout"))
          filter.andThen(next)
        }
      }
    }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]]
   * for use in servers.
   */
  def serverModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[
        TimeoutFilter.Param,
        param.Timer,
        param.Stats,
        ServiceFactory[Req, Rep]] {
      val role = TimeoutFilter.role
      val description = "Apply a timeout-derived deadline to requests; adjust existing deadlines."
      def make(
        _param: Param,
        _timer: param.Timer,
        _stats: param.Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val Param(timeout) = _param
        val param.Timer(timer) = _timer
        val param.Stats(stats) = _stats
        if (!timeout.isFinite || timeout <= Duration.Zero) next else {
          val exc = new IndividualRequestTimeoutException(timeout)
          val filter = new TimeoutFilter[Req, Rep](
            timeout, exc, timer, stats.scope("timeout"))
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
 * A [[com.twitter.finagle.Filter]] that applies a global timeout to requests.
 *
 * @param timeout the timeout to apply to requests
 * @param exception an exception object to return in cases of timeout exceedance
 * @param timer a `Timer` object used to track elapsed time
 *
 * @see The sections on
 *      [[https://twitter.github.io/finagle/guide/Clients.html#timeouts-expiration clients]]
 *      and [[https://twitter.github.io/finagle/guide/Servers.html#request-timeout servers]]
 *      in the user guide for more details.
 */
class TimeoutFilter[Req, Rep](
    timeout: Duration,
    exception: RequestTimeoutException,
    timer: Timer,
    statsReceiver: StatsReceiver)
  extends SimpleFilter[Req, Rep] {

  def this(timeout: Duration, exception: RequestTimeoutException, timer: Timer) =
    this(timeout, exception, timer, NullStatsReceiver)

  def this(timeout: Duration, timer: Timer) =
    this(timeout, new IndividualRequestTimeoutException(timeout), timer)

  private[this] val expiredDeadlineStat = statsReceiver.stat("expired_deadline_ms")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val timeoutDeadline = Deadline.ofTimeout(timeout)

    // If there's a current deadline, we combine it with the one derived
    // from our timeout.
    val deadline = Deadline.current match {
      case Some(current) => Deadline.combined(timeoutDeadline, current)
      case None => timeoutDeadline
    }

    if (deadline.expired) {
      expiredDeadlineStat.add(-deadline.remaining.inMillis)
    }

    Contexts.broadcast.let(Deadline, deadline) {
      val res = service(request)
      res.within(timer, timeout).rescue {
        case exc: java.util.concurrent.TimeoutException =>
          res.raise(exc)
          Trace.record(TimeoutFilter.TimeoutAnnotation)
          Future.exception(exception)
      }
    }
  }
}