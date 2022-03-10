package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.GaugeType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot
import com.twitter.finagle.stats.Snapshot.Percentile
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

  val poolSizeFloatGauge = GaugeSnapshot(
    hierarchicalName = "finagle/future_pool/pool_size_float",
    builder = MetricBuilder(
      name = Seq("finagle", "future_pool", "pool_size_float"),
      metricType = GaugeType,
      units = CustomUnit("Threads"),
      labels = Map("pool" -> "future_pool", "rpc" -> "finagle"),
      statsReceiver = sr
    ),
    value = 3.0
  )

  val poolSizeLongGauge = GaugeSnapshot(
    hierarchicalName = "finagle/future_pool/pool_size_long",
    builder = MetricBuilder(
      name = Seq("finagle", "future_pool", "pool_size_long"),
      metricType = GaugeType,
      units = CustomUnit("Threads"),
      labels = Map("pool" -> "future_pool", "rpc" -> "finagle"),
      statsReceiver = sr
    ),
    value = 3l
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

  val dnsLookupMs = HistogramSnapshot(
    hierarchicalName = "inet/dns/lookup_ms",
    builder = MetricBuilder(
      name = Seq("inet", "dns", "lookup_ms"),
      metricType = HistogramType,
      units = Milliseconds,
      labels = Map("resolver" -> "inet", "namer" -> "dns"),
      statsReceiver = sr
    ),
    value = new Snapshot {
      // 1, 2, 3
      def count: Long = 3
      def sum: Long = 6
      def max: Long = 3
      def min: Long = 1
      def average: Double = 2.0
      def percentiles: IndexedSeq[Snapshot.Percentile] =
        IndexedSeq(Percentile(0.5, 2), Percentile(0.95, 3))
    }
  )

  test("Write labels") {
    val writer = new StringBuilder()
    writeLabels(writer, requestsCounter.builder.labels, true)
    assert(writer.toString() == """{role="foo",job="baz-service",env="staging",zone="dc1"}""")
  }

  test("Write a counter without any labels") {
    val writer = new StringBuilder()
    writeMetrics(writer, counters = Seq(noLabelCounter), gauges = Seq(), histograms = Seq())
    assert(
      writer.toString() ==
        """# TYPE requests counter
          |# UNIT requests Requests
          |requests 1
          |""".stripMargin)
  }

  test("write counter with +/- infinite value") {
    val posInfCounter = CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = CounterType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Long.MaxValue
    )

    val negInfCounter = CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = CounterType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Long.MinValue
    )

    val writer = new StringBuilder()
    writeMetrics(
      writer,
      counters = Seq(posInfCounter, negInfCounter),
      gauges = Seq(),
      histograms = Seq())
    val expected =
      """# TYPE requests_pos counter
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg counter
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(writer.toString() == expected)
  }

  test("write long gauge with +/- infinite value") {
    val posInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = GaugeType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Long.MaxValue
    )

    val negInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = GaugeType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Long.MinValue
    )

    val writer = new StringBuilder()
    writeMetrics(
      writer,
      counters = Seq(),
      gauges = Seq(posInfCounter, negInfCounter),
      histograms = Seq())
    val expected =
      """# TYPE requests_pos gauge
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg gauge
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(writer.toString() == expected)
  }

  test("write float gauge with +/- Float.MaxValue value") {
    val posInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = GaugeType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Float.MaxValue
    )

    val negInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = GaugeType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Float.MinValue
    )

    val writer = new StringBuilder()
    writeMetrics(
      writer,
      counters = Seq(),
      gauges = Seq(posInfCounter, negInfCounter),
      histograms = Seq())
    val expected =
      """# TYPE requests_pos gauge
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg gauge
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(writer.toString() == expected)
  }

  test("write float gauge with +/- Infinity value") {
    val posInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = GaugeType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Float.PositiveInfinity
    )

    val negInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = GaugeType,
        units = Requests,
        labels = Map("role" -> "foo"),
        statsReceiver = sr,
      ),
      value = Float.NegativeInfinity
    )

    val writer = new StringBuilder()
    writeMetrics(
      writer,
      counters = Seq(),
      gauges = Seq(posInfCounter, negInfCounter),
      histograms = Seq())
    val expected =
      """# TYPE requests_pos gauge
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg gauge
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(writer.toString() == expected)
  }

  test("Write all metrics") {
    val writer = new StringBuilder()
    writeMetrics(
      writer,
      counters = Seq(requestsCounter, clntExceptionsCounter),
      gauges = Seq(poolSizeFloatGauge, poolSizeLongGauge),
      histograms = Seq(dnsLookupMs))
    val expected =
      """# TYPE requests counter
        |# UNIT requests Requests
        |requests{role="foo",job="baz-service",env="staging",zone="dc1"} 1
        |# TYPE failures counter
        |# UNIT failures Requests
        |failures{side="clnt",exception="com.twitter.finagle.ChannelClosedException",method_name="get",type="logical",client_label="baz-service"} 2
        |# TYPE pool_size_float gauge
        |# UNIT pool_size_float Threads
        |pool_size_float{pool="future_pool",rpc="finagle"} 3.0
        |# TYPE pool_size_long gauge
        |# UNIT pool_size_long Threads
        |pool_size_long{pool="future_pool",rpc="finagle"} 3
        |# TYPE lookup_ms summary
        |# UNIT lookup_ms Milliseconds
        |lookup_ms{resolver="inet",namer="dns",quantile="0.5"} 2
        |lookup_ms{resolver="inet",namer="dns",quantile="0.95"} 3
        |lookup_ms_count{resolver="inet",namer="dns"} 3
        |lookup_ms_sum{resolver="inet",namer="dns"} 6
        |""".stripMargin
    assert(writer.toString() == expected)
  }
}
