package com.twitter.finagle.stats

import com.google.common.{base => guavaBase}
import com.twitter.common.quantity
import com.twitter.common.stats.WindowedApproxHistogram._
import com.twitter.common.stats
import com.twitter.common.util.{Clock, LowResClock}
import java.lang.{Long => JLong}

private object WindowedBucketedHistogram {

  val Supplier = new guavaBase.Supplier[BucketedHistogram] {
    def get(): BucketedHistogram = BucketedHistogram()
  }

  val Merger = new guavaBase.Function[Array[BucketedHistogram], stats.Histogram] {
    def apply(histograms: Array[BucketedHistogram]): stats.Histogram = {
      BucketedHistogram.merged(histograms)
    }
  }
}


/**
 * Adapter for windowing on top of [[BucketedHistogram BucketedHistograms]].
 *
 * This is ''not'' internally thread-safe and thread safety must be applied
 * externally. Typically, this is done via [[MetricsBucketedHistogram]].
 */
private[stats] class WindowedBucketedHistogram(
    window: quantity.Amount[JLong, quantity.Time] = DEFAULT_WINDOW,
    slices: Int = DEFAULT_SLICES,
    clock: Clock = LowResClock.DEFAULT)
  extends stats.WindowedHistogram[BucketedHistogram](
    classOf[BucketedHistogram],
    window,
    slices,
    WindowedBucketedHistogram.Supplier,
    WindowedBucketedHistogram.Merger,
    clock)
