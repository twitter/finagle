package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.client.LatencyCompensation
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.service.TimeoutFilter.DeadlineAnnotation
import com.twitter.finagle.service.TimeoutFilter.TimeoutAnnotation
import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.Tracing
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Timer
import com.twitter.util.tunable.Tunable
import scala.util.control.NoStackTrace

private[twitter] object DeadlineOnlyToggle {
  private val enableToggle = CoreToggles("com.twitter.finagle.service.DeadlineOnly")
  private val zoneEnabled = ServerInfo().zone.contains("atla")
  @volatile private var overridden: Option[Boolean] = None

  /**
   * This is exposed only to be used in selected tests. If you found this method by accident,
   * there is a VERY high change you won't want to call it.
   */
  def unsafeOverride(enabled: Option[Boolean]): Unit = overridden = enabled

  def apply(trace: Tracing): Boolean = {
    overridden match {
      case Some(o) => o
      case None =>
        zoneEnabled && {
          trace.idOption.flatMap(_._traceId) match {
            case Some(spanId) => enableToggle(spanId.toLong.hashCode())
            case None => false
          }
        }
    }
  }
}

object TimeoutFilter {
  private[finagle] val serverKey = "srv/"
  private[finagle] val clientKey = "clnt/"
  val TimeoutAnnotation: String = "finagle_timeout_ms"
  private[finagle] val DeadlineAnnotation: String = "finagle_deadline_ms"

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
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.service.TimeoutFilter]] module that respects propagated deadline
   * for request cancellation. The default behavior is respecting configured timeouts.
   * @note this is for the toggle to apply to targeted protocols, eg: http/thriftmux
   */
  private[finagle] case class PreferDeadlineOverTimeout(enabled: Boolean)
  private[finagle] object PreferDeadlineOverTimeout {
    val Default: Boolean = false
    implicit val param: Stack.Param[PreferDeadlineOverTimeout] =
      Stack.Param(PreferDeadlineOverTimeout(Default))
  }

  /**
   * Used for adding, or not, a `TimeoutFilter` for `Stack.Module.make`.
   */
  private[finagle] def make[Req, Rep](
    tunable: Tunable[Duration],
    defaultTimeout: Duration,
    propagateDeadlines: Boolean,
    preferDeadlineOverTimeout: Boolean,
    compensation: Duration,
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer,
    statsReceiver: StatsReceiver,
    rolePrefix: String,
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
          propagateDeadlines,
          preferDeadlineOverTimeout,
          statsReceiver,
          rolePrefix
        ).andThen(next)
    }
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]]
   * for use in clients.
   */
  def clientModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module6[
      TimeoutFilter.Param,
      param.Timer,
      PropagateDeadlines,
      PreferDeadlineOverTimeout,
      LatencyCompensation.Compensation,
      param.Stats,
      ServiceFactory[Req, Rep]
    ] {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a timeout-derived deadline to requests; adjust existing deadlines."

      def make(
        param: Param,
        timerParam: com.twitter.finagle.param.Timer,
        propagateDeadlines: PropagateDeadlines,
        preferDeadlineOverTimeout: PreferDeadlineOverTimeout,
        compensation: LatencyCompensation.Compensation,
        stats: Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] =
        TimeoutFilter.make(
          param.tunableTimeout,
          TimeoutFilter.Param.Default,
          propagateDeadlines.enabled,
          preferDeadlineOverTimeout.enabled,
          compensation.howlong,
          timeout => new IndividualRequestTimeoutException(timeout),
          timerParam.timer,
          stats.statsReceiver,
          clientKey,
          next
        )
    }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.TimeoutFilter]]
   * for use in servers.
   */
  def serverModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module4[
      TimeoutFilter.Param,
      param.Timer,
      PreferDeadlineOverTimeout,
      param.Stats,
      ServiceFactory[Req, Rep]
    ] {
      val role: Stack.Role = TimeoutFilter.role
      val description: String =
        "Apply a timeout-derived deadline to requests; adjust existing deadlines."
      def make(
        param: Param,
        timerParam: com.twitter.finagle.param.Timer,
        preferDeadlineOverTimeout: PreferDeadlineOverTimeout,
        stats: Stats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] =
        TimeoutFilter.make(
          param.tunableTimeout,
          TimeoutFilter.Param.Default,
          PropagateDeadlines.Default,
          preferDeadlineOverTimeout.enabled,
          Duration.Zero,
          timeout => new IndividualRequestTimeoutException(timeout),
          timerParam.timer,
          stats.statsReceiver,
          serverKey,
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
    timer: Timer,
    preferDeadlineOverTimeout: Boolean = false,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    rolePrefix: String = ""
  ): TypeAgnostic = new TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new TimeoutFilter[Req, Rep](
        timeoutFn,
        exceptionFn,
        timer,
        PropagateDeadlines.Default,
        preferDeadlineOverTimeout,
        statsReceiver,
        rolePrefix)
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
  propagateDeadlines: Boolean,
  preferDeadlineOverTimeout: Boolean = false,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  rolePrefix: String = "")
    extends SimpleFilter[Req, Rep] {
  def this(
    timeoutFn: () => Duration,
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer
  ) =
    this(
      timeoutFn,
      exceptionFn,
      timer,
      TimeoutFilter.PropagateDeadlines.Default,
      TimeoutFilter.PreferDeadlineOverTimeout.Default)

  def this(
    timeout: Tunable[Duration],
    exceptionFn: Duration => RequestTimeoutException,
    timer: Timer
  ) =
    this(
      () => timeout().getOrElse(TimeoutFilter.Param.Default),
      exceptionFn,
      timer,
      TimeoutFilter.PropagateDeadlines.Default,
      TimeoutFilter.PreferDeadlineOverTimeout.Default
    )

  def this(timeout: Duration, exception: RequestTimeoutException, timer: Timer) =
    this(() => timeout, _ => exception, timer, TimeoutFilter.PropagateDeadlines.Default)

  def this(timeout: Duration, timer: Timer) =
    this(timeout, new IndividualRequestTimeoutException(timeout), timer)

  private[this] val deadlineStat = statsReceiver.stat(
    Some("A histogram of propagated deadlines of requests in milliseconds"),
    "current_deadline")
  private[this] val inExperimentDeadlineCounter =
    statsReceiver.counter(Some("A counter of requests that enabled deadlineOnly"), "deadline_only")
  private[this] val inExperimentDeadlineLtTimeout = statsReceiver.counter(
    Some("Indicates that deadline is stricter than timeout for requests in experiment"),
    "deadline_lt_timeout_experiment")
  private[this] val deadlineLtTimeout = statsReceiver.counter(
    Some("Indicates that deadline is stricter than timeout"),
    "deadline_lt_timeout")

  private[this] val deadlineAnnotation = rolePrefix + DeadlineAnnotation
  private[this] val timeoutAnnotation = rolePrefix + TimeoutAnnotation

  private[this] class InternalTimeoutException extends Exception with NoStackTrace
  private[this] val internalTimeoutEx = new InternalTimeoutException

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val timeout = timeoutFn()
    val timeoutDeadline = Deadline.ofTimeout(timeout)
    var trace: Tracing = null
    val deadlineOnly = preferDeadlineOverTimeout && {
      trace = Trace()
      DeadlineOnlyToggle(trace)
    }

    if (propagateDeadlines) {
      val deadline = Deadline.current match {
        case Some(current) => determineExperiment(trace, deadlineOnly, timeoutDeadline, current)
        case None => timeoutDeadline
      }
      val finalTimeout = if (deadlineOnly) deadline.remaining else timeout
      Contexts.broadcast.let(Deadline, deadline) {
        applyTimeout(request, service, finalTimeout)
      }
    } else {
      Contexts.broadcast.letClear(Deadline) {
        applyTimeout(request, service, timeout)
      }
    }
  }

  // count requests in experiments, record deadline in trace
  // count when deadline is stricter in and out experiment
  private[this] def determineExperiment(
    trace: Tracing,
    deadlineOnly: Boolean,
    timeoutDeadline: Deadline,
    current: Deadline
  ): Deadline = {
    val combined = Deadline.combined(timeoutDeadline, current)
    deadlineStat.add(current.remaining.inMilliseconds)
    if (deadlineOnly) {
      inExperimentDeadlineCounter.incr()
      deadlineCompareTimeout(combined, current, inExperimentDeadlineLtTimeout)
      if (trace.isActivelyTracing) {
        val deadlineRecord = s"timestamp:${current.timestamp}:deadline:${current.deadline}"
        trace.recordBinary(deadlineAnnotation, s"deadline_enabled:$deadlineRecord")
      }
      current
    } else {
      deadlineCompareTimeout(combined, current, deadlineLtTimeout)
      combined
    }
  }

  private[this] def deadlineCompareTimeout(
    combined: Deadline,
    deadline: Deadline,
    deadlineLtTimeoutCounter: Counter
  ): Unit = {
    if (combined.compare(deadline) == 0) {
      deadlineLtTimeoutCounter.incr()
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
      res.within(timer, timeout, internalTimeoutEx).rescue {
        case exc if exc eq internalTimeoutEx =>
          val timeoutEx = exceptionFn(timeout)
          res.raise(timeoutEx)
          Trace.record(timeoutAnnotation)
          Future.exception(timeoutEx)
      }
    }
  }
}
