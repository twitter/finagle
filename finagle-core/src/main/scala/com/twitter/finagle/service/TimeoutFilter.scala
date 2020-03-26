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
  class Param private[twitter] (val tunableTimeout: Tunable[Duration]) {

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

    def apply(tunableTimeout: Tunable[Duration]): Param =
      new Param(tunableTimeout)

    def unapply(param: Param): Option[Duration] = param.tunableTimeout()

    private[twitter] val Default = Duration.Top

    implicit val param: Stack.Param[TimeoutFilter.Param] =
      new Stack.Param[Param] {
        val default = Param(Default)
        override def show(p: Param): Seq[(String, () => String)] =
          Seq(("timeout", () => p.timeout.toString))
      }
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module when used for
   * a total timeout of a logical request, including retries.
   */
  private[finagle] class TotalTimeout private[twitter] (val tunableTimeout: Tunable[Duration]) {

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

    def apply(tunableTimeout: Tunable[Duration]): TotalTimeout =
      new TotalTimeout(tunableTimeout)

    private[finagle] val Default = Duration.Top

    implicit val param: Stack.Param[TotalTimeout] =
      Stack.Param(TotalTimeout(Default))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module when used for
   * enabling propagation of deadlines to outbound requests. The default
   * behavior is to propagate deadlines.
   */
  case class PropagateDeadlines(enabled: Boolean) {
    def mk(): (PropagateDeadlines, Stack.Param[PropagateDeadlines]) =
      (this, PropagateDeadlines.param)
  }

  object PropagateDeadlines {
    val Default: Boolean = true

    implicit val param: Stack.Param[PropagateDeadlines] =
      Stack.Param(PropagateDeadlines(Default))
  }

  /**
   * Used for adding, or not, a `TimeoutFilter` for `Stack.Module.make`.
   */
  private[finagle] def make[Req, Rep](
    tunable: Tunable[Duration],
    defaultTimeout: Duration,
    propagateDeadlines: Boolean,
    compensation: Duration,
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer,
    next: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] = {
    def hasNoTimeout(duration: Duration, compensation: Duration): Boolean = {
      val total = duration + compensation
      !total.isFinite || total <= Duration.Zero
    }

    tunable match {
      case Tunable.Const(duration) if hasNoTimeout(duration, compensation) =>
        next
      case _ =>
        val timeoutFn: () => Duration = () => {
          val tunableTimeout = tunable() match {
            case Some(duration) => duration
            case None => defaultTimeout
          }
          compensation + tunableTimeout
        }
        new TimeoutFilter[Req, Rep](
          timeoutFn,
          exceptionFn,
          timer,
          propagateDeadlines
        ).andThen(next)
    }
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]]
   * for use in clients.
   */
  def clientModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module4[
      TimeoutFilter.Param,
      param.Timer,
      PropagateDeadlines,
      LatencyCompensation.Compensation,
      ServiceFactory[Req, Rep]
    ] {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a timeout-derived deadline to requests; adjust existing deadlines."

      def make(
        param: Param,
        timerParam: com.twitter.finagle.param.Timer,
        propagateDeadlines: PropagateDeadlines,
        compensation: LatencyCompensation.Compensation,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] =
        TimeoutFilter.make(
          param.tunableTimeout,
          TimeoutFilter.Param.Default,
          propagateDeadlines.enabled,
          compensation.howlong,
          timeout => new IndividualRequestTimeoutException(timeout),
          timerParam.timer,
          next
        )
    }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]]
   * for use in servers.
   */
  def serverModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[TimeoutFilter.Param, param.Timer, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a timeout-derived deadline to requests; adjust existing deadlines."
      def make(
        param: Param,
        timerParam: com.twitter.finagle.param.Timer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] =
        TimeoutFilter.make(
          param.tunableTimeout,
          TimeoutFilter.Param.Default,
          PropagateDeadlines.Default,
          Duration.Zero,
          timeout => new IndividualRequestTimeoutException(timeout),
          timerParam.timer,
          next
        )
    }

  def typeAgnostic(
    timeout: Duration,
    exception: RequestTimeoutException,
    timer: Timer
  ): TypeAgnostic =
    typeAgnostic(() => timeout, _ => exception, timer)

  private[twitter] def typeAgnostic(
    timeoutTunable: Tunable[Duration],
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer
  ): TypeAgnostic = {
    val timeoutFn: () => Duration = () =>
      timeoutTunable() match {
        case Some(duration) => duration
        case None => TimeoutFilter.Param.Default
      }
    typeAgnostic(timeoutFn, exceptionFn, timer)
  }

  private[twitter] def typeAgnostic(
    timeoutFn: () => Duration,
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer
  ): TypeAgnostic = new TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new TimeoutFilter[Req, Rep](timeoutFn, exceptionFn, timer, PropagateDeadlines.Default)
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
  timer: Timer,
  propagateDeadlines: Boolean)
    extends SimpleFilter[Req, Rep] {

  def this(
    timeoutFn: () => Duration,
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer
  ) =
    this(timeoutFn, exceptionFn, timer, TimeoutFilter.PropagateDeadlines.Default)

  def this(
    timeout: Tunable[Duration],
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer
  ) =
    this(
      () => timeout().getOrElse(TimeoutFilter.Param.Default),
      exceptionFn,
      timer,
      TimeoutFilter.PropagateDeadlines.Default
    )

  def this(timeout: Duration, exception: RequestTimeoutException, timer: Timer) =
    this(() => timeout, _ => exception, timer, TimeoutFilter.PropagateDeadlines.Default)

  def this(timeout: Duration, timer: Timer) =
    this(timeout, new IndividualRequestTimeoutException(timeout), timer)

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val timeout = timeoutFn()
    val timeoutDeadline = Deadline.ofTimeout(timeout)

    if (propagateDeadlines) {
      // If there's a current deadline, we combine it with the one derived from our timeout.
      val deadline = Deadline.current match {
        case Some(current) => Deadline.combined(timeoutDeadline, current)
        case None => timeoutDeadline
      }

      Contexts.broadcast.let(Deadline, deadline) {
        applyTimeout(request, service, timeout)
      }
    } else {
      Contexts.broadcast.letClear(Deadline) {
        applyTimeout(request, service, timeout)
      }
    }
  }

  private[this] def applyTimeout(
    request: Req,
    service: Service[Req, Rep],
    timeout: Duration
  ): Future[Rep] = {
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
