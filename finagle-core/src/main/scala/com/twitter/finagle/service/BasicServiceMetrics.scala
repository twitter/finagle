package com.twitter.finagle.service

import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.ExceptionStatsHandler
import com.twitter.finagle.stats.Stat
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Throw
import com.twitter.util.Try
import java.util.concurrent.TimeUnit

/**
 * A collection of metrics that are considered key to finagle services
 *
 * @param timeUnit The time units to use for request latency.
 * @param exceptionStatsHandler the properly configured `ExceptionStatsHandler` for recording errors.
 * @param metricsRegistry optional `CoreMetricRegistry` in which to register metrics for expressions.
 * @param statsReceiver A `StatsReceiver` which will be used to instantiate the metrics.
 * @param responseClassifier the `ResponseClassifier` used to categorize responses.
 */
private[finagle] class BasicServiceMetrics(
  timeUnit: TimeUnit,
  exceptionStatsHandler: ExceptionStatsHandler,
  metricsRegistry: Option[CoreMetricsRegistry],
  statsReceiver: StatsReceiver,
  responseClassifier: ResponseClassifier) {

  import BasicServiceMetrics._

  private[this] val requestCount: Counter = statsReceiver.counter(Descriptions.requests, "requests")
  private[this] val successCount: Counter = statsReceiver.counter(Descriptions.success, "success")
  private[this] val latencyStat: Stat =
    statsReceiver.stat(Descriptions.latency, s"request_latency_${latencyStatSuffix(timeUnit)}")

  // ExceptionStatsHandler creates the failure counter lazily.
  // We need to eagerly register this counter in metrics for success rate expression.
  private[this] val failureCountMetadata =
    statsReceiver.counter(Descriptions.failures, ExceptionStatsHandler.Failures).metadata

  // inject metrics and instrument top-line expressions
  metricsRegistry.foreach { registry =>
    Seq(
      registry.SuccessCounter -> successCount.metadata,
      registry.FailureCounter -> failureCountMetadata,
      registry.RequestCounter -> requestCount.metadata,
      registry.LatencyP99Histogram -> latencyStat.metadata
    ).foreach {
      case (k, metadata) =>
        metadata.toMetricBuilder.foreach { metricBuilder =>
          registry.setMetricBuilder(k, metricBuilder, statsReceiver)
        }
    }
  }

  /**
   * Record a service call
   *
   * @param request the request that will be sent to the service
   * @param response the response that was received from the service
   * @param duration the duration of the service call in the time units used to construct this instance
   */
  def recordStats(
    request: Any,
    response: Try[Any],
    duration: Long
  ): Unit = {
    requestCount.incr()
    responseClassifier
      .applyOrElse(ReqRep(request, response), ResponseClassifier.Default) match {
      case ResponseClass.Ignorable => // Do nothing.
      case ResponseClass.Failed(_) =>
        latencyStat.add(duration)
        // exceptionStatsHandler will increment the "failures" counter for us.
        response match {
          case Throw(e) =>
            exceptionStatsHandler.record(statsReceiver, e)
          case _ =>
            exceptionStatsHandler.record(statsReceiver, SyntheticException)
        }
      case ResponseClass.Successful(_) =>
        successCount.incr()
        latencyStat.add(duration)
    }
  }
}

private[finagle] object BasicServiceMetrics {

  private val SyntheticException =
    new ResponseClassificationSyntheticException()

  object Descriptions {
    val requests: Some[String] = Some("A counter of the total number of successes + failures.")
    val success: Some[String] = Some("A counter of the total number of successes.")
    val latency: Some[String] = Some("A histogram of the latency of requests in milliseconds.")
    val failures: Some[String] = Some(
      "A counter of the number of times any failure has been observed.")
  }

  private def latencyStatSuffix(timeUnit: TimeUnit): String = timeUnit match {
    case TimeUnit.NANOSECONDS => "ns"
    case TimeUnit.MICROSECONDS => "us"
    case TimeUnit.MILLISECONDS => "ms"
    case TimeUnit.SECONDS => "secs"
    case _ => timeUnit.toString.toLowerCase
  }
}
