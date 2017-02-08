package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.client.LatencyCompensation
import com.twitter.finagle.context.{Contexts, Deadline}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.tunable.Tunable
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
 class Param private[twitter](val tunableTimeout: Tunable[Duration]) {

    def this(timeout: Duration) =
      this(Tunable.const(role.name, timeout))

    def timeout: Duration = tunableTimeout() match {
      case Some(duration) => duration
      case None => Param.Default
    }

    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  object Param {
    def apply(timeout: Duration): Param =
      new Param(timeout)

    def apply(tunableTimeout: Tunable[Duration]) =
      new Param(tunableTimeout)

    def unapply(param: Param): Option[Duration] = param.tunableTimeout()

    private[twitter] val Default = Duration.Top

    implicit val param: Stack.Param[TimeoutFilter.Param] =
      Stack.Param(Param(Default))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module when used for
   * a total timeout of a logical request, including retries.
   */
  private[finagle] class TotalTimeout private[twitter](val tunableTimeout: Tunable[Duration]) {

    def this(timeout: Duration) =
      this(Tunable.const(role.name, timeout))

    def timeout: Duration = tunableTimeout() match {
      case Some(duration) => duration
      case None => Param.Default
    }

    def mk(): (TotalTimeout, Stack.Param[Param]) =
      (this, Param.param)
  }

  private[finagle] object TotalTimeout {
    def apply(timeout: Duration): TotalTimeout =
      new TotalTimeout(timeout)

    private[finagle] val Default = Duration.Top

    implicit val param: Stack.Param[TotalTimeout] =
      Stack.Param(TotalTimeout(Default))
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
      ): ServiceFactory[Req, Rep] = _param.tunableTimeout match {
        case Tunable.Const(duration)
          if !(duration + _compensation.howlong).isFinite ||
             (duration + _compensation.howlong) <= Duration.Zero =>
          next

        case tunable =>
          val timeoutFn: () => Duration = () => _compensation.howlong + (tunable() match {
            case Some(duration) => duration
            case None => TimeoutFilter.Param.Default
          })

          new TimeoutFilter[Req, Rep](
            timeoutFn,
            timeout => new IndividualRequestTimeoutException(timeout),
            _timer.timer
          ).andThen(next)
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
      ): ServiceFactory[Req, Rep] = _param.tunableTimeout match {
        case Tunable.Const(duration) if !duration.isFinite || duration <= Duration.Zero =>
          next
        case tunable =>
          val timeoutFn: () => Duration = () => tunable() match {
            case Some(duration) => duration
            case None => TimeoutFilter.Param.Default
          }

          new TimeoutFilter[Req, Rep](
            timeoutFn,
            timeout => new IndividualRequestTimeoutException(timeout),
            _timer.timer
          ).andThen(next)
      }
    }

  def typeAgnostic(
    timeout: Duration,
    exception: RequestTimeoutException,
    timer: Timer
  ): TypeAgnostic =
    typeAgnostic(Tunable.const(TimeoutFilter.role.name, timeout), _ => exception, timer)

  private[twitter] def typeAgnostic(
    timeoutTunable: Tunable[Duration],
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer
  ): TypeAgnostic =
    new TypeAgnostic {
      private[this] val timeoutFn: () => Duration = () => timeoutTunable() match {
        case Some(duration) => duration
        case None => TimeoutFilter.Param.Default
      }

      override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
        new TimeoutFilter[Req, Rep](
          timeoutFn,
          timeout => new IndividualRequestTimeoutException(timeout),
          timer)
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
