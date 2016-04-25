package com.twitter.finagle.stats

import com.twitter.common.metrics.{Histogram, HistogramInterface, Percentile, Snapshot}
import com.twitter.concurrent.Once
import com.twitter.conversions.time._
import com.twitter.util.{Duration, Time}
import java.util.concurrent.atomic.AtomicReference

/**
 * Adapts `BucketedHistogram` to the `HistogramInterface`.
 *
 * This is safe to use from multiple threads.
 *
 * @param latchPeriod how often calls to [[snapshot()]]
 *   should trigger a rolling of the collection bucket.
 */
private[stats] class MetricsBucketedHistogram(
    name: String,
    percentiles: Array[Double] = Histogram.DEFAULT_QUANTILES,
    latchPeriod: Duration = MetricsBucketedHistogram.DefaultLatchPeriod)
  extends HistogramInterface
{
  import MetricsBucketedHistogram.{MutableSnapshot, HistogramCountsSnapshot} 
  assert(name.length > 0)

  private[this] val nextSnapAfter = new AtomicReference(Time.Undefined)

  // thread-safety provided via synchronization on `current`
  private[this] val current = BucketedHistogram()
  private[this] val snap = new MutableSnapshot(percentiles)
  private[this] val histogramCountsSnap = new HistogramCountsSnapshot
  // If histograms counts haven't been requested we don't want to be
  // computing them every snapshot
  @volatile private[this] var isHistogramRequested: Boolean = false

  def getName: String = name

  def clear(): Unit = current.synchronized {
    current.clear()
    snap.clear()
  }

  def add(value: Long): Unit = current.synchronized {
    current.add(value)
  }

  /**
   * Produces a 1 minute snapshot of histogram counts 
   */
  private[this] val hd: HistogramDetail = {
    new HistogramDetail {
      private[this] val fn = Once {
        isHistogramRequested = true
        current.synchronized {
          histogramCountsSnap.recomputeFrom(current)
        }
      }

      def counts: Seq[BucketAndCount] = {
        fn()
        histogramCountsSnap.counts
      }
    }
  }

  def histogramDetail: HistogramDetail = hd

  def snapshot(): Snapshot = {
    // at most once per `latchPeriod` after the first call to
    // `snapshot` we roll over the currently captured data in the histogram
    // and begin collecting into a clean histogram. for a duration of `latchPeriod`,
    // requests for the snapshot will return values from the previous `latchPeriod`.

    if (Time.Undefined eq nextSnapAfter.get) {
      nextSnapAfter.compareAndSet(Time.Undefined, JsonExporter.startOfNextMinute)
    }

    current.synchronized {
      // we give 1 second of wiggle room so that a slightly early request
      // will still trigger a roll.
      if (Time.now >= nextSnapAfter.get - 1.second) {
        // if nextSnapAfter have a datetime older than (latchPeriod*2) ago, update it next minutes.
        if (nextSnapAfter.get + latchPeriod*2 > Time.now) {
          nextSnapAfter.set(nextSnapAfter.get + latchPeriod)
        } else {
          nextSnapAfter.set(JsonExporter.startOfNextMinute)
        }
        if (isHistogramRequested) histogramCountsSnap.recomputeFrom(current)
        snap.recomputeFrom(current)
        current.clear()
      }

      new Snapshot {
        // need to capture these variables from `snap` while we have a lock.
        val _count = snap.count
        val _sum = snap.sum
        val _max = snap.max
        val _min = snap.min
        val _avg = snap.avg
        val ps = new Array[Percentile](MetricsBucketedHistogram.this.percentiles.length)
        var i = 0
        while (i < ps.length) {
          ps(i) = new Percentile(MetricsBucketedHistogram.this.percentiles(i), snap.quantiles(i))
          i += 1
        }
        override def count(): Long = _count
        override def max(): Long = _max
        override def percentiles(): Array[Percentile] = ps
        override def avg(): Double = _avg
        override def stddev(): Double = 0.0 // unsupported
        override def min(): Long = _min
        override def sum(): Long = _sum

        override def toString: String = {
          val _ps = ps.map { p =>
            s"p${p.getQuantile}=${p.getValue}"
          }.mkString("[", ", ", "]")

          s"Snapshot(count=${_count}, max=${_max}, min=${_min}, avg=${_avg}, sum=${_sum}, %s=${_ps})"
        }
      }
    }
  }

}

private object MetricsBucketedHistogram {

  private val DefaultLatchPeriod = 1.minute

  /**
   * A mutable struct used to store the most recent calculation
   * of snapshot. By reusing a single instance per Stat allows us to
   * avoid creating objects with medium length lifetimes that would
   * need to exist from one stat collection to the next.
   *
   * NOT THREAD SAFE, and thread-safety must be provided
   * by the MetricsBucketedHistogram that owns a given instance.
   */
  private final class MutableSnapshot(percentiles: Array[Double]) {
    var count = 0L
    var sum = 0L
    var max = 0L
    var min = 0L
    var avg = 0.0
    var quantiles = new Array[Long](percentiles.length)

    def recomputeFrom(histo: BucketedHistogram): Unit = {
      count = histo.count
      sum = histo.sum
      max = histo.maximum
      min = histo.minimum
      avg = histo.average
      quantiles = histo.getQuantiles(percentiles)
    }

    def clear(): Unit = {
      count = 0L
      sum = 0L
      max = 0L
      min = 0L
      avg = 0.0
      java.util.Arrays.fill(quantiles, 0L)
    }
  }

  /** 
   * Stores a mutable reference to Histogram counts.
   * Thread safety needs to be provided on histogram
   * instances passed to recomputeFrom (histogram counts
   * should not be changing while it is called).
   */
  private final class HistogramCountsSnapshot {
    @volatile private[stats] var counts: Seq[BucketAndCount] = Nil

    def recomputeFrom(histo: BucketedHistogram): Unit = 
      counts = histo.bucketAndCounts
  }

}
