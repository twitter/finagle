package com.twitter.finagle.service

import com.twitter.finagle.Service
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.acRejectName
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.deadlineRejectName
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.failuresName
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.latencyName
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.successRateName
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.throughputName
import com.twitter.finagle.service.MetricBuilderRegistry._
import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import com.twitter.finagle.stats.exp.ExpressionSchemaKey
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.Metadata
import com.twitter.finagle.stats.MetricBuilder
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class MetricBuilderRegistryTest extends AnyFunSuite {
  class Ctx {
    val sr = new InMemoryStatsReceiver
    val svc = Service.mk { _: String => Future.value("hi") }
    val metricBuilders = Map[MetricName, Metadata](
      DeadlineRejectedCounter -> MetricBuilder(
        name = Seq("deadline", "rejected"),
        metricType = CounterType,
        statsReceiver = sr),
      ACRejectedCounter -> MetricBuilder(
        name = Seq("admission_control", "rejections"),
        metricType = CounterType,
        statsReceiver = sr),
      SuccessCounter -> MetricBuilder(
        name = Seq("success"),
        metricType = CounterType,
        statsReceiver = sr),
      FailureCounter -> MetricBuilder(
        name = Seq("failures"),
        metricType = CounterType,
        statsReceiver = sr),
      RequestCounter -> MetricBuilder(
        name = Seq("requests"),
        metricType = CounterType,
        statsReceiver = sr),
      LatencyP99Histogram -> MetricBuilder(
        name = Seq("latency"),
        metricType = HistogramType,
        statsReceiver = sr)
    )
  }

  private[this] def nameToKey(
    name: String,
    labels: Map[String, String] = Map()
  ): ExpressionSchemaKey =
    ExpressionSchemaKey(name, labels, Seq())

  test("Expression Factory generates all expressions when metrics are injected") {
    new Ctx {
      val mbr = new MetricBuilderRegistry()

      metricBuilders.map {
        case (name, metricBuilder) =>
          mbr.setMetricBuilder(name, metricBuilder)
      }

      assert(sr.expressions.isEmpty)

      mbr.successRate
      mbr.throughput
      mbr.latencyP99
      mbr.deadlineRejection
      mbr.acRejection
      mbr.failures

      assert(sr.expressions.size == 6)
      assert(sr.expressions.contains(nameToKey(successRateName)))
      assert(sr.expressions.contains(nameToKey(throughputName)))
      assert(sr.expressions.contains(nameToKey(latencyName, Map("bucket" -> "p99"))))
      assert(sr.expressions.contains(nameToKey(deadlineRejectName)))
      assert(sr.expressions.contains(nameToKey(acRejectName)))
      assert(sr.expressions.contains(nameToKey(failuresName)))
    }
  }

  test("no-op when any needed metrics is non-present") {
    new Ctx {
      val metricBuildersMissing = metricBuilders - RequestCounter
      val mbr = new MetricBuilderRegistry()
      metricBuildersMissing.map {
        case (name, metricBuilder) =>
          mbr.setMetricBuilder(name, metricBuilder)
      }

      mbr.successRate
      mbr.throughput
      mbr.latencyP99
      mbr.deadlineRejection
      mbr.acRejection

      assert(sr.expressions.size == 2)
      assert(sr.expressions.contains(nameToKey(successRateName)))
      assert(sr.expressions.contains(nameToKey(latencyName, Map("bucket" -> "p99"))))
    }
  }
}
