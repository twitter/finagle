package com.twitter.finagle.service

import com.twitter.conversions.PercentOps._
import com.twitter.finagle.stats._
import com.twitter.finagle.stats.exp.DefaultExpression
import com.twitter.finagle.stats.exp.Expression
import com.twitter.finagle.stats.exp.ExpressionSchema
import com.twitter.finagle.stats.exp.HistogramComponent

private[twitter] object CoreMetricsRegistry {

  object ExpressionNames {
    val deadlineRejectName = "deadline_rejection_rate"
    val acRejectName = "throttling_ac_rejection_rate"
  }

}

/**
 * MetricBuilderRegistry holds a set of essential metrics that are injected through
 * other finagle stack modules. It provides means of instrumenting top-line expressions.
 */
final class CoreMetricsRegistry extends MetricBuilderRegistry {

  import CoreMetricsRegistry.ExpressionNames

  case object SuccessCounter extends MetricName
  case object FailureCounter extends MetricName
  case object RequestCounter extends MetricName
  case object LatencyP99Histogram extends MetricName
  case object ACRejectedCounter extends MetricName
  case object DeadlineRejectedCounter extends MetricName

  private[this] def rejection(
    name: String,
    request: Expression,
    reject: Expression
  ): ExpressionSchema =
    ExpressionSchema(name, Expression(100).multiply(reject.divide(request)))
      .withUnit(Percentage)
      .withDescription(s"Default $name rejection rate.")

  defineBuilder(Set(SuccessCounter, FailureCounter)) { results =>
    val success = results(SuccessCounter)
    val failure = results(FailureCounter)
    DefaultExpression.successRate(Expression(success), Expression(failure))
  }

  defineBuilder(Set(RequestCounter)) { results =>
    DefaultExpression.throughput(Expression(results(RequestCounter)))
  }

  defineBuilder(Set(LatencyP99Histogram)) { builders =>
    DefaultExpression
      .latency99(
        Expression(builders(LatencyP99Histogram), HistogramComponent.Percentile(99.percent)))
  }

  defineBuilder(Set(RequestCounter, DeadlineRejectedCounter)) { builders =>
    val request = builders(RequestCounter)
    val reject = builders(DeadlineRejectedCounter)
    rejection(ExpressionNames.deadlineRejectName, Expression(request), Expression(reject))
  }

  defineBuilder(Set(RequestCounter, ACRejectedCounter)) { builders =>
    val request = builders(RequestCounter)
    val reject = builders(ACRejectedCounter)
    rejection(ExpressionNames.acRejectName, Expression(request), Expression(reject))
  }

  defineBuilder(Set(FailureCounter)) { builders =>
    val failures = builders(FailureCounter)
    DefaultExpression.failures(Expression(failures, true))
  }
}
