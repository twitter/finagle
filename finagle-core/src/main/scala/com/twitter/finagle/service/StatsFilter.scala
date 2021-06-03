package com.twitter.finagle.service

import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.service.MetricBuilderRegistry.{
  FailureCounter,
  LatencyP99Histogram,
  RequestCounter,
  SuccessCounter
}
import com.twitter.finagle.stats.MetricBuilder.{CounterType, HistogramType}
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
    new Stack.Module6[
      param.Stats,
      param.ExceptionStatsHandler,
      param.ResponseClassifier,
      Param,
      Now,
      param.MetricBuilders,
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
            metrics.registry
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

  private val SyntheticException =
    new ResponseClassificationSyntheticException()

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
  metricsRegistry: Option[MetricBuilderRegistry] = None)
    extends SimpleFilter[Req, Rep] {
  import StatsFilter.SyntheticException

  /**
   * A `StatsFilter` reports request statistics including number of requests,
   * number successful, and request latency to the given [[StatsReceiver]].
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
   * measuring latency.  The default is milliseconds,
   * but other values are valid. The choice of this changes the name of the stat
   * attached to the given [[StatsReceiver]]. For the common units,
   * it will be "request_latency_ms".
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
    metricBuilderRegistry: MetricBuilderRegistry
  ) = {
    this(
      statsReceiver,
      ResponseClassifier.Default,
      exceptionStatsHandler,
      TimeUnit.MILLISECONDS,
      StatsFilter.nowForTimeUnit(TimeUnit.MILLISECONDS),
      Some(metricBuilderRegistry))
  }

  private[this] def latencyStatSuffix: String = {
    timeUnit match {
      case TimeUnit.NANOSECONDS => "ns"
      case TimeUnit.MICROSECONDS => "us"
      case TimeUnit.MILLISECONDS => "ms"
      case TimeUnit.SECONDS => "secs"
      case _ => timeUnit.toString.toLowerCase
    }
  }

  private[this] val successSchema =
    MetricBuilder(
      name = Seq("success"),
      metricType = CounterType,
      statsReceiver = statsReceiver).withKernel
  private[this] val failureSchema =
    MetricBuilder(
      name = Seq(ExceptionStatsHandler.Failures),
      metricType = CounterType,
      statsReceiver = statsReceiver).withKernel
  private[this] val requestSchema =
    MetricBuilder(
      name = Seq("requests"),
      metricType = CounterType,
      statsReceiver = statsReceiver).withKernel
  private[this] val latencySchema =
    MetricBuilder(
      name = Seq(s"request_latency_$latencyStatSuffix"),
      metricType = HistogramType,
      statsReceiver = statsReceiver).withKernel

  private[this] val outstandingRequestCount = new LongAdder()
  private[this] val dispatchCount = statsReceiver.counter(requestSchema)
  private[this] val successCount = statsReceiver.counter(successSchema)
  // ExceptionStatsHandler creates the failure counter lazily.
  // We need to eagerly register this counter in metrics for success rate expression.
  private[this] val failureCount = statsReceiver.counter(failureSchema)
  private[this] val latencyStat = statsReceiver.stat(latencySchema)
  private[this] val outstandingRequestCountGauge =
    statsReceiver.addGauge("pending") { outstandingRequestCount.sum() }

  // inject metrics and instrument top-line expressions
  private[this] val instrumentExpressions = metricsRegistry.map { registry =>
    registry.setMetricBuilder(SuccessCounter, successCount.metadata)
    registry.setMetricBuilder(FailureCounter, failureCount.metadata)
    registry.setMetricBuilder(RequestCounter, dispatchCount.metadata)
    registry.setMetricBuilder(LatencyP99Histogram, latencyStat.metadata)

    // Construct expressions
    registry.successRate
    registry.latencyP99
    registry.throughput
    registry.acRejection
  }

  private[this] def isIgnorableResponse(rep: Try[Rep]): Boolean = rep match {
    case Throw(f: FailureFlags[_]) if f.isFlagged(FailureFlags.Ignorable) =>
      true
    case _ =>
      false
  }

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val start = now()

    outstandingRequestCount.increment()

    val result =
      try {
        service(request)
      } catch {
        case NonFatal(e) =>
          Future.exception(e)
      }

    result.respond { response =>
      outstandingRequestCount.decrement()
      if (!isIgnorableResponse(response)) {
        dispatchCount.incr()
        responseClassifier.applyOrElse(
          ReqRep(request, response),
          ResponseClassifier.Default
        ) match {
          case ResponseClass.Ignorable => // Do nothing.
          case ResponseClass.Failed(_) =>
            latencyStat.add(now() - start)
            response match {
              case Throw(e) =>
                exceptionStatsHandler.record(statsReceiver, e)
              case _ =>
                exceptionStatsHandler.record(statsReceiver, SyntheticException)
            }
          case ResponseClass.Successful(_) =>
            successCount.incr()
            latencyStat.add(now() - start)
        }
      }
    }
  }
}

private[finagle] object StatsServiceFactory {
  val role: Stack.Role = Stack.Role("FactoryStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.StatsServiceFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = StatsServiceFactory.role
      val description: String = "Report connection statistics"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new StatsServiceFactory(next, statsReceiver)
      }
    }
}

class StatsServiceFactory[Req, Rep](factory: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver)
    extends ServiceFactoryProxy[Req, Rep](factory) {
  private[this] val availableGauge = statsReceiver.addGauge("available") {
    if (isAvailable) 1f else 0f
  }
}
