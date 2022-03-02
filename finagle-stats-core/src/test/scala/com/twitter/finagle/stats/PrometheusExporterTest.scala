package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.CounterType
import org.scalatest.funsuite.AnyFunSuite

class PrometheusExporterTest extends AnyFunSuite {
  import PrometheusExporter._

  val noLabelCounter = MetricFamilySample(
    name = "requests",
    metricType = CounterType,
    metricUnit = Some(Requests),
    value = 1,
    labels = Seq()
  )

  val requestsCounter = MetricFamilySample(
    name = "requests",
    metricType = CounterType,
    metricUnit = Some(Requests),
    value = 1,
    labels = Seq(("role", "foo"), ("job", "baz-service"), ("env", "staging"), ("zone", "dc1"))
  )

  val clntExceptionsCounter = MetricFamilySample(
    name = "failures",
    metricType = CounterType,
    metricUnit = Some(Requests),
    value = 2,
    labels = Seq(
      ("side", "clnt"),
      ("client_label", "baz-service"),
      ("method_name", "get"),
      ("type", "logical"),
      ("exception", "com.twitter.finagle.ChannelClosedException"))
  )

  test("Write labels") {
    val writer = new StringBuilder()
    writeLabels(writer, requestsCounter.labels)
    assert(writer.toString() == """{role="foo",job="baz-service",env="staging",zone="dc1"}""")
  }

  test("Write a counter without any labels") {
    val writer = new StringBuilder()
    writeCounters(writer, Seq(noLabelCounter))
    assert(
      writer.toString() ==
        """# TYPE requests counter
        |# UNIT requests Requests
        |requests 1
        |""".stripMargin)
  }

  test("Write counters") {
    val writer = new StringBuilder()
    writeCounters(writer, Seq(requestsCounter, clntExceptionsCounter))
    val expected =
      """# TYPE requests counter
        |# UNIT requests Requests
        |requests{role="foo",job="baz-service",env="staging",zone="dc1"} 1
        |# TYPE failures counter
        |# UNIT failures Requests
        |failures{side="clnt",client_label="baz-service",method_name="get",type="logical",exception="com.twitter.finagle.ChannelClosedException"} 2
        |""".stripMargin
    assert(writer.toString() == expected)
  }
}
