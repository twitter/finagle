package com.twitter.finagle.stats

import com.twitter.concurrent.Once
import com.twitter.conversions.DurationOps._
import com.twitter.util.Duration
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicReference

/**
 * A [[MetricsHistogram]] that is latched such that a snapshot of
 * the values are taken every `latchPeriod` and that value is returned
 * for rest of `latchPeriod`. This gives pull based collectors a
 * simple way to get consistent results.
 *
 * This is safe to use from multiple threads.
 *
 * @param latchPeriod how often calls to [[snapshot()]]
 *   should trigger a rolling of the collection bucket.
 */
class MetricsBucketedHistogram(
  name: String,
  percentiles: IndexedSeq[Double] = BucketedHistogram.DefaultQuantiles,
  latchPeriod: Duration = MetricsBucketedHistogram.DefaultLatchPeriod,
  useLockFreeBucketedHistogram: Boolean = false)
    extends MetricsHistogram {
  import MetricsBucketedHistogram.HistogramCountsSnapshot
  assert(name.length > 0)

  private[this] val nextSnapAfter = new AtomicReference(Time.Undefined)

  private[this] val current: AbstractBucketedHistogram = {
    if (useLockFreeBucketedHistogram) new LockFreeBucketedHistogram()
    else BucketedHistogram()
  }
  private[this] val snap = new BucketedHistogram.MutableSnapshot(percentiles)
  private[this] val histogramCountsSnap = new HistogramCountsSnapshot
  // If histograms counts haven't been requested we don't want to be
  // computing them every snapshot
  @volatile private[this] var isHistogramRequested: Boolean = false

  def getName: String = name

  def clear(): Unit = {
    current.clear()
    snap.clear()
  }

  def add(value: Long): Unit = {
    current.add(value)
  }

  /**
   * Produces a 1 minute snapshot of histogram counts
   */
  private[this] val hd: HistogramDetail = {
    new HistogramDetail {
      private[this] val fn = Once {
        isHistogramRequested = true
        histogramCountsSnap.recomputeFrom(current)
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
      nextSnapAfter.compareAndSet(Time.Undefined, Time.now + latchPeriod)
    }

    // we give 1 second of wiggle room so that a slightly early request
    // will still trigger a roll.
    val now = Time.now
    val prev = nextSnapAfter.get
    if (now >= prev - 1.second) {
      // if nextSnapAfter has a datetime older than (latchPeriod*2) ago, update it after next minutes.
      val next = if (prev + latchPeriod * 2 > now) {
        prev + latchPeriod
      } else {
        now + latchPeriod
      }

      if (nextSnapAfter.compareAndSet(prev, next)) {
        if (isHistogramRequested) histogramCountsSnap.recomputeFrom(current)
        current.recompute(snap)
        current.clear()
      }
    }

    new Snapshot {
      // this is slightly racy because we don't hold the read lock
      val _count = snap.count
      val _sum = snap.sum
      val _max = snap.max
      val _min = snap.min
      val _avg = snap.avg
      val ps = {
        val ps = new Array[Snapshot.Percentile](MetricsBucketedHistogram.this.percentiles.length)
        var i = 0
        while (i < ps.length) {
          ps(i) =
            new Snapshot.Percentile(MetricsBucketedHistogram.this.percentiles(i), snap.quantiles(i))
          i += 1
        }
        ps
      }
      def count: Long = _count
      def max: Long = _max
      def percentiles: IndexedSeq[Snapshot.Percentile] = ps
      def average: Double = _avg
      def min: Long = _min
      def sum: Long = _sum

      override def toString: String = {
        val _ps = ps
          .map { p => s"p${p.quantile}=${p.value}" }
          .mkString("[", ", ", "]")
        s"Snapshot(count=${_count}, max=${_max}, min=${_min}, avg=${_avg}, sum=${_sum}, %s=${_ps})"
      }
    }
  }

}

private object MetricsBucketedHistogram {

  private val DefaultLatchPeriod = 1.minute

  /**
   * Stores a mutable reference to Histogram counts.
   * Thread safety needs to be provided on histogram
   * instances passed to recomputeFrom (histogram counts
   * should not be changing while it is called).
   */
  private final class HistogramCountsSnapshot {
    @volatile private[stats] var counts: Seq[BucketAndCount] = Nil

    def recomputeFrom(histo: AbstractBucketedHistogram): Unit =
      counts = histo.bucketAndCounts
  }

}
