package com.twitter.finagle.service

import com.twitter.conversions.PercentOps._
import com.twitter.finagle.service.MetricBuilderRegistry._
import com.twitter.finagle.stats._
import com.twitter.finagle.stats.exp.DefaultExpression
import com.twitter.finagle.stats.exp.Expression
import com.twitter.finagle.stats.exp.ExpressionSchema
import com.twitter.finagle.stats.exp.HistogramComponent
import java.util.concurrent.atomic.AtomicReference

private[twitter] object MetricBuilderRegistry {

  object ExpressionNames {
    val deadlineRejectName = "deadline_rejection_rate"
    val acRejectName = "throttling_ac_rejection_rate"
  }

  sealed trait MetricName
  case object SuccessCounter extends MetricName
  case object FailureCounter extends MetricName
  case object RequestCounter extends MetricName
  case object LatencyP99Histogram extends MetricName
  case object ACRejectedCounter extends MetricName
  case object DeadlineRejectedCounter extends MetricName
}

/**
 * MetricBuilderRegistry holds a set of essential metrics that are injected through
 * other finagle stack modules. It provides means of instrumenting top-line expressions.
 */
private[twitter] class MetricBuilderRegistry {

  private[this] val successCounter: AtomicReference[Metadata] = new AtomicReference(NoMetadata)
  private[this] val failureCounter: AtomicReference[Metadata] = new AtomicReference(NoMetadata)
  private[this] val requestCounter: AtomicReference[Metadata] = new AtomicReference(NoMetadata)
  private[this] val latencyP99Histogram: AtomicReference[Metadata] =
    new AtomicReference(NoMetadata)
  private[this] val aCRejectedCounter: AtomicReference[Metadata] = new AtomicReference(NoMetadata)
  private[this] val deadlineRejectedCounter: AtomicReference[Metadata] =
    new AtomicReference(NoMetadata)

  private[this] def getRef(metricName: MetricName): AtomicReference[Metadata] = {
    metricName match {
      case SuccessCounter => successCounter
      case FailureCounter => failureCounter
      case RequestCounter => requestCounter
      case LatencyP99Histogram => latencyP99Histogram
      case ACRejectedCounter => aCRejectedCounter
      case DeadlineRejectedCounter => deadlineRejectedCounter
    }
  }

  /**
   * Set the metric once when we obtain a valid metric builder
   */
  def setMetricBuilder(metricName: MetricName, metricBuilder: Metadata): Boolean = {
    if (metricBuilder != NoMetadata) {
      getRef(metricName).compareAndSet(NoMetadata, metricBuilder)
    } else false
  }

  // no operation when any needed MetricBuilder is not valid
  lazy val successRate: Unit = {
    val successMb = successCounter.get().toMetricBuilder
    val failureMb = failureCounter.get().toMetricBuilder
    (successMb, failureMb) match {
      case (Some(success), Some(failure)) =>
        DefaultExpression.successRate(Expression(success), Expression(failure)).build()
      case _ => // no-op if any wanted metric is not found or results in NoMetadata
    }
  }

  lazy val throughput: Unit = {
    requestCounter.get().toMetricBuilder match {
      case Some(request) =>
        DefaultExpression.throughput(Expression(request)).build()
      case _ => // no-op
    }
  }

  lazy val latencyP99: Unit = {
    latencyP99Histogram.get().toMetricBuilder match {
      case Some(latencyP99) =>
        DefaultExpression
          .latency99(Expression(latencyP99, HistogramComponent.Percentile(99.percent))).build()
      case _ => // no-op
    }
  }

  lazy val deadlineRejection: Unit = {
    val requestMb = requestCounter.get().toMetricBuilder
    val rejectionMb = deadlineRejectedCounter.get().toMetricBuilder
    (requestMb, rejectionMb) match {
      case (Some(request), Some(reject)) =>
        rejection(ExpressionNames.deadlineRejectName, Expression(request), Expression(reject))
          .build()
      case _ => // no-op
    }
  }

  lazy val acRejection: Unit = {
    val requestMb = requestCounter.get().toMetricBuilder
    val rejectionMb = aCRejectedCounter.get().toMetricBuilder
    (requestMb, rejectionMb) match {
      case (Some(request), Some(reject)) =>
        rejection(ExpressionNames.acRejectName, Expression(request), Expression(reject)).build()
      case _ => // no-op
    }
  }

  lazy val failures: Unit = {
    failureCounter.get().toMetricBuilder match {
      case Some(failures) =>
        DefaultExpression.failures(Expression(failures, true)).build()
      case None => //no-op
    }
  }

  private[this] def rejection(
    name: String,
    request: Expression,
    reject: Expression
  ): ExpressionSchema =
    ExpressionSchema(name, Expression(100).multiply(reject.divide(request)))
      .withUnit(Percentage)
      .withDescription(s"Default $name rejection rate.")
}
