package com.twitter.finagle.stats

import com.twitter.common.metrics.{Histogram, HistogramInterface, Percentile, Snapshot}
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
    latchPeriod: Duration = 1.minute)
  extends HistogramInterface
{
  assert(name.length > 0)

  private[this] val nextSnapAfter = new AtomicReference(Time.Undefined)

  // thread-safety provided via synchronization on `this`
  private[this] val a = BucketedHistogram()
  private[this] val b = BucketedHistogram()
  private[this] var current = a

  def getName: String = name

  def clear(): Unit = synchronized {
    a.clear()
    b.clear()
  }

  def add(value: Long): Unit = synchronized {
    current.add(value)
  }

  def snapshot(): Snapshot = {
    // at most once per `latchPeriod` after the first call to
    // `snapshot` we roll over the currently captured data in the histogram
    // and begin collecting into a clean histogram. for a duration of `latchPeriod`,
    // requests for the snapshot will return values from the previous `latchPeriod`.

    if (Time.Undefined eq nextSnapAfter.get) {
      nextSnapAfter.compareAndSet(Time.Undefined, JsonExporter.startOfNextMinute)
    }

    synchronized {
      val prev =
        // we give 1 second of wiggle room so that a slightly early request
        // will still trigger a roll.
        if (Time.now < nextSnapAfter.get - 1.second) {
          // not yet time to roll, so keep reading from previous
          if (current eq a) b else a
        } else {
          // time to roll to the next histogram, clearing it out before allowing usage.
          nextSnapAfter.set(nextSnapAfter.get + latchPeriod)
          val (prev, next) = if (current eq a) (a, b) else (b, a)
          next.clear()
          current = next
          prev
        }

      // need to capture these variables from `prev` while we have a lock.
      val _count = prev.count
      val _sum = prev.sum
      val _max = prev.maximum
      val _min = prev.minimum
      val _avg = prev.average
      val quantiles = prev.getQuantiles(percentiles)
      val ps = percentiles.zip(quantiles).map { case (p, q) =>
        new Percentile(p, q)
      }
      new Snapshot {
        override def count(): Long = _count
        override def max(): Long = _max
        override def percentiles(): Array[Percentile] = ps
        override def avg(): Double = _avg
        override def stddev(): Double = 0.0 // unsupported
        override def min(): Long = _min
        override def sum(): Long = _sum
      }
    }
  }

}
