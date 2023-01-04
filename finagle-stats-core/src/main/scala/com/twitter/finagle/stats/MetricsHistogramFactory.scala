package com.twitter.finagle.stats

object MetricsHistogramFactory {

  type Type = (MetricsHistogramFactory.Params) => MetricsHistogram

  case class Params(
    name: String,
    percentiles: IndexedSeq[Double] = BucketedHistogram.DefaultQuantiles,
    hints: Set[MetricUsageHint] = Set.empty)
}
