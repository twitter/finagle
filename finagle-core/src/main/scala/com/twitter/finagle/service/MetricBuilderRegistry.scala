package com.twitter.finagle.service

import com.twitter.conversions.PercentOps._
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames._
import com.twitter.finagle.service.MetricBuilderRegistry._
import com.twitter.finagle.stats._
import com.twitter.finagle.stats.exp.{Expression, ExpressionSchema, GreaterThan, MonotoneThresholds}
import java.util.concurrent.atomic.AtomicReference

private[twitter] object MetricBuilderRegistry {

  object ExpressionNames {
    val successRateName = "success_rate"
    val throughputName = "throughput"
    val latencyName = "latency"
    val deadlineRejectName = "deadline_rejection_rate"
    val acRejectName = "throttling_ac_rejection_rate"
  }

  private val descriptionSuffix = "constructed by MetricBuilderRegistry"

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
    val successMb = Metadata.getMetricBuilder(successCounter.get())
    val failureMb = Metadata.getMetricBuilder(failureCounter.get())
    (successMb, failureMb) match {
      case (Some(success), Some(failure)) =>
        ExpressionSchema(
          successRateName,
          Expression(100).multiply(
            Expression(success).divide(Expression(success).plus(Expression(failure)))))
          .withBounds(MonotoneThresholds(GreaterThan, 99.5, 99.97))
          .withUnit(Percentage)
          .withDescription(s"The success rate expression $descriptionSuffix.")
          .register()
      case _ => // no-op if any wanted metric is not found or results in NoMetadata
    }
  }

  lazy val throughput: Unit = {
    Metadata.getMetricBuilder(requestCounter.get()) match {
      case Some(request) =>
        ExpressionSchema(throughputName, Expression(request))
          .withUnit(Requests)
          .withDescription(s"The total requests expression $descriptionSuffix.")
          .register()
      case _ => // no-op
    }
  }

  lazy val latencyP99: Unit = {
    Metadata.getMetricBuilder(latencyP99Histogram.get()) match {
      case Some(latencyP99) =>
        ExpressionSchema(latencyName, Expression(latencyP99, Right(99.percent)))
          .withUnit(Milliseconds)
          .withDescription(s"The p99 latency of a request $descriptionSuffix.")
          .register()
      case _ => // no-op
    }
  }

  lazy val deadlineRejection: Unit = {
    val requestMb = Metadata.getMetricBuilder(requestCounter.get())
    val rejectionMb = Metadata.getMetricBuilder(deadlineRejectedCounter.get())
    (requestMb, rejectionMb) match {
      case (Some(request), Some(reject)) =>
        ExpressionSchema(
          deadlineRejectName,
          Expression(100).multiply(Expression(reject).divide(Expression(request))))
          .withUnit(Percentage)
          .withDescription(s"Deadline Filter rejection rate $descriptionSuffix.")
          .register()
      case _ => // no-op
    }
  }

  lazy val acRejection: Unit = {
    val requestMb = Metadata.getMetricBuilder(requestCounter.get())
    val rejectionMb = Metadata.getMetricBuilder(aCRejectedCounter.get())
    (requestMb, rejectionMb) match {
      case (Some(request), Some(reject)) =>
        ExpressionSchema(
          acRejectName,
          Expression(100).multiply(Expression(reject).divide(Expression(request))))
          .withUnit(Percentage)
          .withDescription(s"Admission Control rejection rate $descriptionSuffix.")
          .register()
      case _ => // no-op
    }
  }

}
