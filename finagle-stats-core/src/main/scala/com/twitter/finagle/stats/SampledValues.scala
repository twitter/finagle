package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot

/**
 * Struct representing the sampled values from
 * the Metrics registry.
 */
private[twitter] case class SampledValues(
  gauges: Iterable[GaugeSnapshot],
  counters: Iterable[CounterSnapshot],
  histograms: Iterable[HistogramSnapshot])
