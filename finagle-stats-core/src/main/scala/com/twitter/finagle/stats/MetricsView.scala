package com.twitter.finagle.stats

import com.twitter.finagle.stats
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot

/**
 * Provides snapshots of metrics values.
 */
private[stats] trait MetricsView {

  /**
   * A read-only snapshot of instantaneous values for all gauges.
   */
  def gauges: Iterable[GaugeSnapshot]

  /**
   * A read-only snapshot of instantaneous values for all counters.
   */
  def counters: Iterable[CounterSnapshot]

  /**
   * A read-only snapshot of instantaneous values for all histograms.
   */
  def histograms: Iterable[HistogramSnapshot]
}

private[stats] object MetricsView {

  /** A snapshot of a metric */
  sealed trait Snapshot {

    /**
     * The name used to present this metric in its hierarchical form.
     *
     * @note currently this exists for performance reasons: it is useful to
     *       form the hierarchical name once on metric creation and use it
     *       on every emission. It also currently serves as the way to determine
     *       if a metric is unique.
     */
    val hierarchicalName: String

    /** The `MetricBuilder` that created this metric. */
    val builder: MetricBuilder
  }

  /** Snapshot representation of a gauge */
  final case class GaugeSnapshot(
    hierarchicalName: String,
    builder: MetricBuilder,
    value: Number)
      extends Snapshot

  /** Snapshot representation of a counter */
  final case class CounterSnapshot(
    hierarchicalName: String,
    builder: MetricBuilder,
    value: Long)
      extends Snapshot

  /** Snapshot representation of a histogram */
  final case class HistogramSnapshot(
    hierarchicalName: String,
    builder: MetricBuilder,
    value: stats.Snapshot)
      extends Snapshot

  private def merge[K <: Snapshot](i1: Iterable[K], i2: Iterable[K]): Iterable[K] = {
    val merged = new java.util.HashSet[Object]()
    val result = scala.collection.mutable.ArrayBuffer.newBuilder[K]

    val accumulate: K => Unit = { k =>
      if (merged.add(k.hierarchicalName)) {
        result += k
      }
    }

    i1.foreach(accumulate)
    i2.foreach(accumulate)

    result.result()
  }

  /**
   * Creates a combined view over the inputs. Note that order matters here
   * and should any key be duplicated, the value from the first view (`view1`)
   * is used.
   */
  def of(view1: MetricsView, view2: MetricsView): MetricsView = new MetricsView {
    def gauges: Iterable[GaugeSnapshot] =
      merge(view1.gauges, view2.gauges)

    def counters: Iterable[CounterSnapshot] =
      merge(view1.counters, view2.counters)

    def histograms: Iterable[HistogramSnapshot] =
      merge(view1.histograms, view2.histograms)
  }
}
