package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.GaugeType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import com.twitter.finagle.stats.MetricBuilder.IdentityType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot
import com.twitter.finagle.stats.Snapshot.Percentile

// Collection of sample snapshots for use with tests
private object SampleSnapshots {

  val NoLabelCounter: CounterSnapshot =
    CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests"),
        metricType = CounterType,
        units = Requests,
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
      ).withDimensionalSupport,
      value = 1
    )

  val RequestsCounter: CounterSnapshot =
    CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests"),
        metricType = CounterType,
        units = Requests
      ).withLabels(
          Map("role" -> "foo", "job" -> "baz-service", "env" -> "staging", "zone" -> "dc1"))
        .withDimensionalSupport,
      value = 1
    )

  val PoolSizeFloatGauge: GaugeSnapshot = GaugeSnapshot(
    hierarchicalName = "finagle/future_pool/pool_size_float",
    builder = MetricBuilder(
      metricType = GaugeType,
      units = CustomUnit("Threads")
    ).withIdentity(
      MetricBuilder.Identity(
        hierarchicalName = Seq("finagle", "future_pool", "pool_size_float"),
        dimensionalName = Seq("pool_size_float"),
        labels = Map("pool" -> "future_pool", "rpc" -> "finagle"),
        identityType = IdentityType.Full
      )),
    value = 3.0
  )

  val poolSizeLongGauge: GaugeSnapshot = GaugeSnapshot(
    hierarchicalName = "finagle/future_pool/pool_size_long",
    builder = MetricBuilder(
      metricType = GaugeType,
      units = CustomUnit("Threads")
    ).withIdentity(
      MetricBuilder.Identity(
        hierarchicalName = Seq("finagle", "future_pool", "pool_size_long"),
        dimensionalName = Seq("pool_size_long"),
        labels = Map("pool" -> "future_pool", "rpc" -> "finagle"),
        identityType = IdentityType.Full
      )),
    value = 3l
  )

  val ClntExceptionsCounter: CounterSnapshot = CounterSnapshot(
    hierarchicalName =
      "clnt/baz-service/get/logical/failures/com.twitter.finagle.ChannelClosedException",
    builder = MetricBuilder(
      metricType = CounterType,
      units = Requests,
      name = Seq("failures")
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

  val HistoSample: HistogramSnapshot = HistogramSnapshot(
    hierarchicalName = "foo/bar/ping_ms",
    builder = MetricBuilder(
      metricType = HistogramType,
      units = Milliseconds
    ).withIdentity(
      MetricBuilder.Identity(
        hierarchicalName = Seq("foo", "bar", "ping_ms"),
        dimensionalName = Seq("foo", "ping_ms"),
        labels = Map("biz" -> "bar"),
        identityType = IdentityType.Full
      )
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

  val EmptyHistoSample: HistogramSnapshot = HistoSample.copy(
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
