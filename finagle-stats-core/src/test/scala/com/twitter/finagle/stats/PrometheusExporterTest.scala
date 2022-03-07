package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.GaugeType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import org.scalatest.funsuite.AnyFunSuite

class PrometheusExporterTest extends AnyFunSuite {
  import PrometheusExporter._

  val sr = new InMemoryStatsReceiver
  val noLabelCounter =
    CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests"),
        metricType = CounterType,
        units = Requests,
        labels = Map[String, String](),
        statsReceiver = sr,
      ),
      value = 1
    )

  val requestsCounter =
    CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests"),
        metricType = CounterType,
        units = Requests,
        labels = Map("role" -> "foo", "job" -> "baz-service", "env" -> "staging", "zone" -> "dc1"),
        statsReceiver = sr,
      ),
      value = 1
    )

  val poolSizeGauge = GaugeSnapshot(
    hierarchicalName = "finagle/future_pool/pool_size",
    builder = MetricBuilder(
      name = Seq("finagle", "future_pool", "pool_size"),
      metricType = GaugeType,
      units = CustomUnit("Threads"),
      labels = Map("pool" -> "future_pool", "rpc" -> "finagle"),
      statsReceiver = sr
    ),
    value = 3.0
  )

  val clntExceptionsCounter = CounterSnapshot(
    hierarchicalName =
      "clnt/baz-service/get/logical/failures/com.twitter.finagle.ChannelClosedException",
    builder = MetricBuilder(
      metricType = CounterType,
      units = Requests,
      name = Seq("failures"),
      labels = Map(
        "side" -> "clnt",
        "client_label" -> "baz-service",
        "method_name" -> "get",
        "type" -> "logical",
        "exception" -> "com.twitter.finagle.ChannelClosedException"),
      statsReceiver = sr,
    ),
    value = 2
  )

  test("Write labels") {
    val writer = new StringBuilder()
    writeLabels(writer, requestsCounter.builder.labels)
    assert(writer.toString() == """{role="foo",job="baz-service",env="staging",zone="dc1"}""")
  }

  test("Write a counter without any labels") {
    val writer = new StringBuilder()
    writeMetrics(writer, counters = Seq(noLabelCounter), gauges = Seq())
    assert(
      writer.toString() ==
        """# TYPE requests counter
          |# UNIT requests Requests
          |requests 1
          |""".stripMargin)
  }
  test("Write all metrics") {
    val writer = new StringBuilder()
    writeMetrics(
      writer,
      counters = Seq(requestsCounter, clntExceptionsCounter),
      gauges = Seq(poolSizeGauge))
    val expected =
      """# TYPE requests counter
        |# UNIT requests Requests
        |requests{role="foo",job="baz-service",env="staging",zone="dc1"} 1
        |# TYPE failures counter
        |# UNIT failures Requests
        |failures{side="clnt",exception="com.twitter.finagle.ChannelClosedException",method_name="get",type="logical",client_label="baz-service"} 2
        |# TYPE pool_size gauge
        |# UNIT pool_size Threads
        |pool_size{pool="future_pool",rpc="finagle"} 3.0
        |""".stripMargin
    assert(writer.toString() == expected)
  }
}
