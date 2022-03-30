package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot

private class TestMetricsView(
  counterSnaps: Iterable[CounterSnapshot],
  gaugeSnaps: Iterable[GaugeSnapshot],
  histoSnaps: Iterable[HistogramSnapshot])
    extends MetricsView {
  override def gauges: Iterable[GaugeSnapshot] = gaugeSnaps
  override def counters: Iterable[CounterSnapshot] = counterSnaps
  override def histograms: Iterable[HistogramSnapshot] = histoSnaps
}
