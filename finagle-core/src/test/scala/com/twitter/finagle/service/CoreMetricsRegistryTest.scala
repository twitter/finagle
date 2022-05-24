package com.twitter.finagle.service

import com.twitter.finagle.Service
import com.twitter.finagle.service.CoreMetricsRegistry.ExpressionNames.acRejectName
import com.twitter.finagle.service.CoreMetricsRegistry.ExpressionNames.deadlineRejectName
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.MetricBuilder
import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import com.twitter.finagle.stats.exp.ExpressionNames.failuresName
import com.twitter.finagle.stats.exp.ExpressionNames.latencyName
import com.twitter.finagle.stats.exp.ExpressionNames.successRateName
import com.twitter.finagle.stats.exp.ExpressionNames.throughputName
import com.twitter.util.Future
import com.twitter.util.testing.ExpressionTestMixin
import org.scalatest.funsuite.AnyFunSuite

class CoreMetricsRegistryTest extends AnyFunSuite with ExpressionTestMixin {

  val sr = new InMemoryStatsReceiver
  val downstreamLabel = Map()

  class Ctx {
    val registry = new CoreMetricsRegistry()
    import registry._

    val svc = Service.mk { _: String => Future.value("hi") }
    val metricBuilders = Map[MetricName, MetricBuilder](
      DeadlineRejectedCounter -> MetricBuilder(
        name = Seq("deadline", "rejected"),
        metricType = CounterType),
      ACRejectedCounter -> MetricBuilder(
        name = Seq("admission_control", "rejections"),
        metricType = CounterType),
      SuccessCounter -> MetricBuilder(name = Seq("success"), metricType = CounterType),
      FailureCounter -> MetricBuilder(name = Seq("failures"), metricType = CounterType),
      RequestCounter -> MetricBuilder(name = Seq("requests"), metricType = CounterType),
      LatencyP99Histogram -> MetricBuilder(name = Seq("latency"), metricType = HistogramType)
    )
  }

  test("Expression Factory generates all expressions when metrics are injected") {
    new Ctx {
      metricBuilders.map {
        case (name, metricBuilder) =>
          registry.setMetricBuilder(name, metricBuilder, sr)
      }

      assert(sr.expressions.size == 6)
      assertExpressionIsRegistered(successRateName)
      assertExpressionIsRegistered(throughputName)
      assertExpressionIsRegistered(latencyName, Map("bucket" -> "p99"))
      assertExpressionIsRegistered(deadlineRejectName)
      assertExpressionIsRegistered(acRejectName)
      assertExpressionIsRegistered(failuresName)
    }
  }

  test("no-op when any needed metrics is non-present") {
    new Ctx {
      val metricBuildersMissing = metricBuilders - registry.RequestCounter
      metricBuildersMissing.map {
        case (name, metricBuilder) =>
          registry.setMetricBuilder(name, metricBuilder, sr)
      }

      assert(sr.expressions.size == 3)
      assertExpressionsAsExpected(
        Set(
          nameToKey(successRateName),
          nameToKey(failuresName),
          nameToKey(latencyName, Map("bucket" -> "p99")))
      )
    }
  }
}
