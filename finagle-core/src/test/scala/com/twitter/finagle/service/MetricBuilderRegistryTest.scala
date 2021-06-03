package com.twitter.finagle.service

import com.twitter.finagle.Service
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.{
  acRejectName,
  deadlineRejectName,
  latencyName,
  successRateName,
  throughputName
}
import com.twitter.finagle.service.MetricBuilderRegistry._
import com.twitter.finagle.stats.MetricBuilder.{CounterType, HistogramType}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, Metadata, MetricBuilder}
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

      assert(sr.expressions.size == 5)
      assert(sr.expressions.contains(successRateName))
      assert(sr.expressions.contains(throughputName))
      assert(sr.expressions.contains(latencyName))
      assert(sr.expressions.contains(deadlineRejectName))
      assert(sr.expressions.contains(acRejectName))
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
      assert(sr.expressions.contains(successRateName))
      assert(sr.expressions.contains(latencyName))
    }
  }
}
