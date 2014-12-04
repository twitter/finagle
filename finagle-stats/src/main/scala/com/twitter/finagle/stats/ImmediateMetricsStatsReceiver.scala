package com.twitter.finagle.stats

import com.twitter.common.metrics._
import com.twitter.common.stats.{Statistics, ApproximateHistogram}
import com.twitter.util.events.Sink

object ImmediateMetricsStatsReceiver {
  def newHistogramInterface(name: String): HistogramInterface = {
    new HistogramInterface {
      private[this] val hist = new ApproximateHistogram()
      private[this] val stats = new Statistics()

      override def snapshot(): Snapshot = synchronized {
        new Snapshot {
          override def avg(): Double = stats.mean()
          override def count(): Long = stats.populationSize()
          override def min(): Long = stats.min()
          override def max(): Long = stats.max()
          override def stddev(): Double = stats.standardDeviation()
          override def sum(): Long = stats.sum()

          override def percentiles(): Array[Percentile] = {
            val quantiles = Histogram.DEFAULT_QUANTILES
            (hist.getQuantiles(quantiles) zip quantiles.toSeq) map {
              case (q, p) => new Percentile(p, q)
            }
          }
        }
      }

      override def getName: String = name

      override def clear(): Unit = synchronized {
        stats.clear()
        hist.clear()
      }

      override def add(n: Long): Unit = synchronized {
        stats.accumulate(n)
        hist.add(n)
      }
    }
  }
}


/**
 * This implementation of MetricsStatsReceiver that doesn't use WindowedApproximateHistogram
 * but ApproximateHistogram.
 * Any value added is immediately aggregated in the result.
 */
class ImmediateMetricsStatsReceiver(registry: Metrics)
  extends MetricsStatsReceiver(registry, Sink.default, ImmediateMetricsStatsReceiver.newHistogramInterface) {

  def this() = this(MetricsStatsReceiver.defaultRegistry)
}
