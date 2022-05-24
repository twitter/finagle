package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.GaugeType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot
import org.scalatest.funsuite.AnyFunSuite

class MetricsViewTest extends AnyFunSuite {

  private[this] def counterMetricBuilder(name: String): MetricBuilder = {
    MetricBuilder(verbosity = Verbosity.Debug, name = Seq(name), metricType = CounterType)
  }

  private[this] def gaugeMetricBuilder(name: String): MetricBuilder = {
    MetricBuilder(verbosity = Verbosity.Debug, name = Seq(name), metricType = GaugeType)
  }

  private[this] def histogramMetricBuilder(name: String): MetricBuilder = {
    MetricBuilder(verbosity = Verbosity.Debug, name = Seq(name), metricType = HistogramType)
  }

  private val EmptySnapshot: Snapshot = new Snapshot {
    def count: Long = 0L
    def sum: Long = 0L
    def max: Long = 0L
    def min: Long = 0L
    def average: Double = 0L
    def percentiles: IndexedSeq[Snapshot.Percentile] = IndexedSeq.empty
  }

  private[this] def counters(pairs: (String, Long)*): Iterable[MetricsView.CounterSnapshot] = {
    pairs.map {
      case (name, value) =>
        MetricsView.CounterSnapshot(name, counterMetricBuilder(name), value)
    }
  }

  private[this] def gauges(pairs: (String, Double)*): Iterable[MetricsView.GaugeSnapshot] = {
    pairs.map {
      case (name, value) =>
        MetricsView.GaugeSnapshot(name, gaugeMetricBuilder(name), value)
    }
  }

  private[this] def histograms(
    pairs: (String, Snapshot)*
  ): Iterable[MetricsView.HistogramSnapshot] = {
    pairs.map {
      case (name, value) =>
        MetricsView.HistogramSnapshot(name, histogramMetricBuilder(name), value)
    }
  }

  private class Impl(
    val gauges: Iterable[GaugeSnapshot] = Nil,
    val counters: Iterable[CounterSnapshot] = Nil,
    val histograms: Iterable[HistogramSnapshot] = Nil)
      extends MetricsView

  private val a1 = new Impl(
    gauges = gauges("a" -> 1),
    counters = counters("a" -> 1),
    histograms = histograms("a" -> EmptySnapshot)
  )

  private val b2 = new Impl(
    gauges = gauges("b" -> 2),
    counters = counters("b" -> 2),
    histograms = histograms("b" -> EmptySnapshot)
  )

  test("of") {
    val aAndB = MetricsView.of(a1, b2)
    assert(gauges("a" -> 1, "b" -> 2) == aAndB.gauges)
    assert(counters("a" -> 1, "b" -> 2) == aAndB.counters)
    assert(histograms("a" -> EmptySnapshot, "b" -> EmptySnapshot) == aAndB.histograms)
  }

  test("of handles duplicates") {
    val c = new Impl(
      gauges = gauges("a" -> 2),
      counters = counters("a" -> 2),
      histograms = histograms("a" -> EmptySnapshot)
    )
    val aAndC = MetricsView.of(a1, c)
    assert(gauges("a" -> 1) == aAndC.gauges)
    assert(counters("a" -> 1) == aAndC.counters)
    assert(histograms("a" -> EmptySnapshot) == aAndC.histograms)

    val cAndA = MetricsView.of(c, a1)
    assert(gauges("a" -> 2) == cAndA.gauges)
    assert(counters("a" -> 2) == cAndA.counters)
    assert(histograms("a" -> EmptySnapshot) == cAndA.histograms)
  }

  test("of ignores empty maps") {
    val empty = new Impl()
    val aAndEmpty = MetricsView.of(a1, empty)
    assert(gauges("a" -> 1) == aAndEmpty.gauges)
    assert(counters("a" -> 1) == aAndEmpty.counters)
    assert(histograms("a" -> EmptySnapshot) == aAndEmpty.histograms)

    val emptyAndA = MetricsView.of(empty, a1)
    assert(gauges("a" -> 1) == emptyAndA.gauges)
    assert(counters("a" -> 1) == emptyAndA.counters)
    assert(histograms("a" -> EmptySnapshot) == emptyAndA.histograms)
  }
}
