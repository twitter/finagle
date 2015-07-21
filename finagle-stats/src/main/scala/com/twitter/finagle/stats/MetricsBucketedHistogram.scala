package com.twitter.finagle.stats

import com.twitter.common.metrics.{Histogram, HistogramInterface, Percentile, Snapshot}
import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Timer
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Adapts `BucketedHistogram` to the `HistogramInterface`.
 *
 * This is safe to use from multiple threads.
 */
private[stats] class MetricsBucketedHistogram(
    name: String,
    percentiles: Array[Double] = Histogram.DEFAULT_QUANTILES,
    timer: Timer = DefaultTimer.twitter)
  extends HistogramInterface
{
  assert(name.length > 0)

  // Thread safety:
  // --------------
  // 1. Because `BucketedHistogram` is not thread safe, we serialize
  //    access to it here.
  // 2. `current` points to where current data collection occurs.
  //    It is marked volatile so that we can safely update the pointer
  //    and have other threads see the change. Updates to `current`
  //    are only done by the `timer` thread so we should not have
  //    concurrent updates.
  // 3. Any access to `a` or `b` must by synchronized on itself to
  //    in order to keep the rule in 1. Typically this is via
  //    synchronization on `current`.
  private[this] val a = BucketedHistogram()
  private[this] val b = BucketedHistogram()
  @volatile private[this] var current = a

  private[this] val latchingStarted = new AtomicBoolean(false)

  def getName: String = name

  def clear(): Unit = {
    a.synchronized { a.clear() }
    b.synchronized { b.clear() }
  }

  def add(value: Long): Unit = {
    current.synchronized {
      // explicit return needed to avoid scala having to get and
      // return a BoxedUnit. sigh, scala (◞‸◟；)
      return current.add(value)
    }
  }

  def snapshot(): Snapshot = {
    // once a minute after the first call to `snapshot` we
    // roll over the currently captured data in the histogram
    // and begin collecting into a clean histogram. for the
    // next minute, requests for the `snapshot` will return
    // values from the previous minute.
    if (latchingStarted.compareAndSet(false, true)) {
      timer.schedule(JsonExporter.startOfNextMinute, 1.minute) {
        // roll to the new histogram, clearing it out before allowing usage.
        //
        // holding the lock on `current` for as short a period as possible.
        // note that due to thread-safety rule #2 above, this is the only
        // thread that can update `current` and therefore there is no risk of
        // deadlock.
        val next = current.synchronized {
          if (current eq a) b else a
        }
        next.synchronized {
          next.clear()
          current.synchronized {
            current = next
          }
        }
      }
    }

    val prev = current.synchronized {
      if (current eq a) b else a
    }
    prev.synchronized {
      val _count = prev.count
      val _sum = prev.sum
      val _max = prev.maximum
      val _min = prev.minimum
      val _avg = prev.average
      val quantiles = prev.getQuantiles(percentiles)
      val ps = percentiles.zip(quantiles).map { case (p, q) =>
        new Percentile(p, q)
      }
      val snap = new Snapshot {
        override def count(): Long = _count
        override def max(): Long = _max
        override def percentiles(): Array[Percentile] = ps
        override def avg(): Double = _avg
        override def stddev(): Double = 0.0 // unsupported
        override def min(): Long = _min
        override def sum(): Long = _sum
      }
      prev.clear()
      snap
    }
  }

}
