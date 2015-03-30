package com.twitter.finagle.stats

import com.twitter.common.metrics.{Histogram, Percentile, HistogramInterface, Snapshot}
import com.twitter.common.quantity
import com.twitter.common.stats.WindowedApproxHistogram.{DEFAULT_SLICES, DEFAULT_WINDOW}
import com.twitter.common.stats.WindowedStatistics
import com.twitter.common.util.{LowResClock, Clock}
import java.lang.{Long => JLong}

/**
 * Adapts WindowedBucketedHistogram's to the `HistogramInterface`
 * and includes some additional properties such as max, min,
 * average.
 *
 * This is safe to use from multiple threads.
 */
private[twitter] class MetricsBucketedHistogram(
    name: String,
    window: quantity.Amount[JLong, quantity.Time] = DEFAULT_WINDOW,
    slices: Int = DEFAULT_SLICES,
    percentiles: Array[Double] = Histogram.DEFAULT_QUANTILES,
    clock: Clock = LowResClock.DEFAULT)
  extends HistogramInterface
{
  assert(name.length > 0)

  private[this] val windowedHisto =
    new WindowedBucketedHistogram(window, slices, clock)

  private[this] val windowedStats =
    new WindowedStatistics(window, slices, clock) {
      override def clear(): Unit = {
        getCurrent().clear()
        getTenured().foreach(_.clear())
      }
    }

  def getName: String = name

  def clear(): Unit = synchronized {
    windowedHisto.clear()
    windowedStats.clear()
  }

  def add(value: Long): Unit = synchronized {
    windowedHisto.add(value)
    windowedStats.accumulate(value)
  }

  def snapshot(): Snapshot = synchronized {
    windowedStats.refresh()
    val _count = windowedStats.populationSize()
    val _sum = windowedStats.sum()
    val _avg = windowedStats.mean()
    val _min = if (_count == 0) 0L else windowedStats.min()
    val _max = if (_count == 0) 0L else windowedStats.max()
    val _stddev = windowedStats.standardDeviation()
    val quantiles = windowedHisto.getQuantiles(percentiles)
    val ps = percentiles.zip(quantiles).map { case (p, q) =>
      new Percentile(p, q)
    }

    new Snapshot {
      override def count(): Long = _count
      override def max(): Long = _max
      override def percentiles(): Array[Percentile] = ps
      override def avg(): Double = _avg
      override def stddev(): Double = _stddev
      override def min(): Long = _min
      override def sum(): Long = _sum
    }
  }

}
