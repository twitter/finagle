package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.GaugeType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot
import com.twitter.finagle.stats.Snapshot.Percentile

// Collection of sample snapshots for use with tests
private object SampleSnapshots {

  val sr = new InMemoryStatsReceiver

  val NoLabelCounter: CounterSnapshot =
    CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests"),
        metricType = CounterType,
        units = Requests,
        statsReceiver = sr,
      ).withDimensionalSupport,
      value = 1
    )

  val DebugCounter: CounterSnapshot =
    CounterSnapshot(
      hierarchicalName = "debug_requests",
      builder = MetricBuilder(
        name = Seq("debug_requests"),
        metricType = CounterType,
        units = Requests,
        verbosity = Verbosity.Debug,
        statsReceiver = sr,
      ).withDimensionalSupport,
      value = 1
    )

  val RequestsCounter: CounterSnapshot =
    CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests"),
        metricType = CounterType,
        units = Requests,
        statsReceiver = sr,
      ).withLabels(
          Map("role" -> "foo", "job" -> "baz-service", "env" -> "staging", "zone" -> "dc1"))
        .withDimensionalSupport,
      value = 1
    )

  val PoolSizeFloatGauge: GaugeSnapshot = GaugeSnapshot(
    hierarchicalName = "finagle/future_pool/pool_size_float",
    builder = MetricBuilder(
      name = Seq("finagle", "future_pool", "pool_size_float"),
      metricType = GaugeType,
      units = CustomUnit("Threads"),
      statsReceiver = sr
    ).withLabels(Map("pool" -> "future_pool", "rpc" -> "finagle")).withDimensionalSupport,
    value = 3.0
  )

  val poolSizeLongGauge: GaugeSnapshot = GaugeSnapshot(
    hierarchicalName = "finagle/future_pool/pool_size_long",
    builder = MetricBuilder(
      name = Seq("finagle", "future_pool", "pool_size_long"),
      metricType = GaugeType,
      units = CustomUnit("Threads"),
      statsReceiver = sr
    ).withLabels(Map("pool" -> "future_pool", "rpc" -> "finagle")).withDimensionalSupport,
    value = 3l
  )

  val ClntExceptionsCounter: CounterSnapshot = CounterSnapshot(
    hierarchicalName =
      "clnt/baz-service/get/logical/failures/com.twitter.finagle.ChannelClosedException",
    builder = MetricBuilder(
      metricType = CounterType,
      units = Requests,
      name = Seq("failures"),
      statsReceiver = sr,
    ).withLabels(
        Map(
          "side" -> "clnt",
          "client_label" -> "baz-service",
          "method_name" -> "get",
          "type" -> "logical",
          "exception" -> "com.twitter.finagle.ChannelClosedException"))
      .withDimensionalSupport,
    value = 2
  )

  val DnsLookupMs: HistogramSnapshot = HistogramSnapshot(
    hierarchicalName = "inet/dns/lookup_ms",
    builder = MetricBuilder(
      name = Seq("inet", "dns", "lookup_ms"),
      metricType = HistogramType,
      units = Milliseconds,
      statsReceiver = sr
    ).withLabels(Map("resolver" -> "inet", "namer" -> "dns")).withDimensionalSupport,
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

  val EmptyDnsLookupMs: HistogramSnapshot = HistogramSnapshot(
    hierarchicalName = "inet/dns/lookup_ms",
    builder = MetricBuilder(
      name = Seq("inet", "dns", "lookup_ms"),
      metricType = HistogramType,
      units = Milliseconds,
      statsReceiver = sr
    ).withLabels(Map("resolver" -> "inet", "namer" -> "dns")).withDimensionalSupport,
    value = new Snapshot {
      def count: Long = 0
      def sum: Long = 0
      def max: Long = 0
      def min: Long = 0
      def average: Double = 0.0
      def percentiles: IndexedSeq[Snapshot.Percentile] =
        IndexedSeq(Percentile(0.5, 0), Percentile(0.95, 0))
    }
  )
}
