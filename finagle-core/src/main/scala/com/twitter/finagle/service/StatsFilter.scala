package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.service.StatsFilter.Descriptions
import com.twitter.finagle.stats._
import com.twitter.util._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder
import scala.util.control.NonFatal

object StatsFilter {
  val role: Stack.Role = Stack.Role("RequestStats")

  /**
   * Configures a [[StatsFilter.module]] to track latency using the
   * given [[TimeUnit]].
   */
  case class Param(unit: TimeUnit) {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  object Param {
    implicit val param = Stack.Param(Param(TimeUnit.MILLISECONDS))
  }

  /**
   * Allows customizing how `now` is computed.
   * Exposed for testing purposes.
   *
   * Defaults to `None` which uses the System clock for timings.
   */
  private[finagle] case class Now(private val nowOpt: Option[() => Long]) {
    def nowOrDefault(timeUnit: TimeUnit): () => Long = nowOpt match {
      case Some(n) => n
      case None => nowForTimeUnit(timeUnit)
    }
  }

  private[finagle] object Now {
    implicit val param: Stack.Param[Now] = Stack.Param(Now(None))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.StatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module7[
      param.Stats,
      param.ExceptionStatsHandler,
      param.ResponseClassifier,
      Param,
      Now,
      param.MetricBuilders,
      param.StandardStats,
      ServiceFactory[Req, Rep]
    ] {
      val role: Stack.Role = StatsFilter.role
      val description: String = "Report request statistics"
      def make(
        _stats: param.Stats,
        _exceptions: param.ExceptionStatsHandler,
        _classifier: param.ResponseClassifier,
        _param: Param,
        now: Now,
        metrics: param.MetricBuilders,
        standardStats: param.StandardStats,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull)
          next
        else {
          new StatsFilter(
            statsReceiver,
            _classifier.responseClassifier,
            _exceptions.categorizer,
            _param.unit,
            now.nowOrDefault(_param.unit),
            metrics.registry,
            standardStats.standardStats
          ).andThen(next)
        }
      }
    }

  /** Basic categorizer with all exceptions under 'failures'. */
  val DefaultExceptions = new MultiCategorizingExceptionStatsHandler(
    mkFlags = FailureFlags.flagsOf,
    mkSource = SourcedException.unapply
  ) {

    override def toString: String = "DefaultCategorizer"
  }

  private object Descriptions {
    val pending: Some[String] = Some("A gauge of the current total number of outstanding requests.")
  }

  def typeAgnostic(
    statsReceiver: StatsReceiver,
    exceptionStatsHandler: ExceptionStatsHandler
  ): TypeAgnostic =
    typeAgnostic(
      statsReceiver,
      ResponseClassifier.Default,
      exceptionStatsHandler,
      TimeUnit.MILLISECONDS
    )

  def typeAgnostic(
    statsReceiver: StatsReceiver,
    responseClassifier: ResponseClassifier,
    exceptionStatsHandler: ExceptionStatsHandler,
    timeUnit: TimeUnit
  ): TypeAgnostic =
    typeAgnostic(
      statsReceiver,
      responseClassifier,
      exceptionStatsHandler,
      timeUnit,
      nowForTimeUnit(timeUnit))

  private[finagle] def typeAgnostic(
    statsReceiver: StatsReceiver,
    responseClassifier: ResponseClassifier,
    exceptionStatsHandler: ExceptionStatsHandler,
    timeUnit: TimeUnit,
    now: () => Long
  ): TypeAgnostic = new TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new StatsFilter[Req, Rep](
        statsReceiver,
        responseClassifier,
        exceptionStatsHandler,
        timeUnit,
        now)
  }

  private def nowForTimeUnit(timeUnit: TimeUnit): () => Long = timeUnit match {
    case TimeUnit.NANOSECONDS => Stopwatch.systemNanos
    case TimeUnit.MICROSECONDS => Stopwatch.systemMicros
    case TimeUnit.MILLISECONDS => Stopwatch.systemMillis
    case _ =>
      () => timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }
}

/**
 * A `StatsFilter` reports request statistics including number of requests,
 * number successful and request latency to the given [[StatsReceiver]].
 *
 * This constructor is exposed for testing purposes.
 *
 * @param responseClassifier used to determine when a response
 * is successful or not.
 *
 * @param timeUnit this controls what granularity is used for
 * measuring latency.  The default is milliseconds,
 * but other values are valid. The choice of this changes the name of the stat
 * attached to the given [[StatsReceiver]]. For the common units,
 * it will be "request_latency_ms".
 *
 * @param metricsRegistry an optional [MetricBuilderRegistry] set by stack parameter
 * for injecting metrics and instrumenting top-line expressions
 */
class StatsFilter[Req, Rep] private[service] (
  statsReceiver: StatsReceiver,
  responseClassifier: ResponseClassifier,
  exceptionStatsHandler: ExceptionStatsHandler,
  timeUnit: TimeUnit,
  now: () => Long,
  metricsRegistry: Option[CoreMetricsRegistry] = None,
  standardStats: StandardStats = Disabled)
    extends SimpleFilter[Req, Rep] {

  /**
   * A `StatsFilter` reports request statistics including number of requests,
   * number successful, and request latency to the given [[StatsReceiver]].
   *
   * This constructor is exposed for testing purposes.
   *
   * @param responseClassifier used to determine when a response
   *                           is successful or not.
   * @param timeUnit           this controls what granularity is used for
   *                           measuring latency.  The default is milliseconds,
   *                           but other values are valid. The choice of this changes the name of the stat
   *                           attached to the given [[StatsReceiver]]. For the common units,
   *                           it will be "request_latency_ms".
   */
  def this(
    statsReceiver: StatsReceiver,
    responseClassifier: ResponseClassifier,
    exceptionStatsHandler: ExceptionStatsHandler,
    timeUnit: TimeUnit
  ) =
    this(
      statsReceiver,
      responseClassifier,
      exceptionStatsHandler,
      timeUnit,
      StatsFilter.nowForTimeUnit(timeUnit))

  /**
   * A `StatsFilter` reports request statistics including number of requests,
   * number successful and request latency to the given [[StatsReceiver]].
   *
   * @param timeUnit this controls what granularity is used for
   *                 measuring latency.  The default is milliseconds,
   *                 but other values are valid. The choice of this changes the name of the stat
   *                 attached to the given [[StatsReceiver]]. For the common units,
   *                 it will be "request_latency_ms".
   */
  def this(
    statsReceiver: StatsReceiver,
    exceptionStatsHandler: ExceptionStatsHandler,
    timeUnit: TimeUnit
  ) = this(statsReceiver, ResponseClassifier.Default, exceptionStatsHandler, timeUnit)

  /**
   * A `StatsFilter` reports request statistics including number of requests,
   * number successful and request latency to the given [[StatsReceiver]].
   */
  def this(statsReceiver: StatsReceiver, exceptionStatsHandler: ExceptionStatsHandler) =
    this(statsReceiver, exceptionStatsHandler, TimeUnit.MILLISECONDS)

  /**
   * A `StatsFilter` reports request statistics including number of requests,
   * number successful and request latency to the given [[StatsReceiver]].
   */
  def this(statsReceiver: StatsReceiver) = this(statsReceiver, StatsFilter.DefaultExceptions)

  //expose for testing
  private[service] def this(
    statsReceiver: StatsReceiver,
    exceptionStatsHandler: ExceptionStatsHandler,
    metricBuilderRegistry: CoreMetricsRegistry
  ) = {
    this(
      statsReceiver,
      ResponseClassifier.Default,
      exceptionStatsHandler,
      TimeUnit.MILLISECONDS,
      StatsFilter.nowForTimeUnit(TimeUnit.MILLISECONDS),
      Some(metricBuilderRegistry))
  }

  // Instantiates the standard set of stats for the cases of standard service metrics and the
  // regular metrics. For the metrics registry we only want to register the configured metrics.
  // Ideally we'd register the standard service metrics but these are not well used yet.
  private[this] def mkBasicStats(
    registerMetrics: Boolean,
    statsReceiver: StatsReceiver,
    responseClassifier: ResponseClassifier
  ): BasicServiceMetrics =
    new BasicServiceMetrics(
      timeUnit,
      exceptionStatsHandler,
      if (registerMetrics) metricsRegistry else None,
      statsReceiver,
      responseClassifier)

  private[this] val standardServiceMetrics: Option[BasicServiceMetrics] = {
    standardStats match {
      case StatsAndClassifier(stdStatsReceiver, responseClassifier) =>
        Some(mkBasicStats(registerMetrics = false, stdStatsReceiver, responseClassifier))
      case StatsOnly(stdStatsReceiver) =>
        Some(mkBasicStats(registerMetrics = false, stdStatsReceiver, ResponseClassifier.Default))
      case Disabled => None
    }
  }

  private[this] val registeredMetrics: BasicServiceMetrics =
    mkBasicStats(registerMetrics = true, statsReceiver, responseClassifier)

  private[this] val outstandingRequestCount = new LongAdder()
  private[this] val outstandingRequestCountGauge =
    statsReceiver.addGauge(Descriptions.pending, "pending") {
      outstandingRequestCount.sum()
    }

  private[this] def isIgnorableResponse(rep: Try[Rep]): Boolean = rep match {
    case Throw(f: FailureFlags[_]) => f.isFlagged(FailureFlags.Ignorable)
    case _ => false
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val start = now()
    outstandingRequestCount.increment()
    val result =
      try service(request)
      catch { case NonFatal(e) => Future.exception(e) }

    result.respond { response =>
      outstandingRequestCount.decrement()
      if (!isIgnorableResponse(response)) {
        val duration = now() - start
        registeredMetrics.recordStats(request, response, duration)

        standardServiceMetrics match {
          case Some(stats) =>
            stats.recordStats(request, response, duration)
          case None => // no-op
        }
      }
    }
  }
}
