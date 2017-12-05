package com.twitter.finagle.exp

import com.twitter.util.WindowedAdder
import java.util.concurrent.locks.StampedLock

private[finagle] object LatencyHistogram {

  /** Default number of slices to use for time windowed adder */
  val DefaultSlices = 5

}

/**
 * A windowed, thread-safe, histogram implementation.
 *
 * This histogram has no dynamic range - it
 * must be configured a priori; but this is
 * appropriate for its application to response
 * latency histograms.
 *
 * None of the methods on LatencyHistogram entails an allocation,
 * unless invoking `now` does.
 *
 * @note `clipDuration`, `history`, and `now` are expected to have the same units.
 *
 * @param clipDuration the maximum duration to measure
 *
 * @param error the allowed error percent for calculating quantiles.
 *   If `0.0`, then the granularity will be one unit of `clipDuration`.
 *   If `(0.0, 1.0]` then it is used as a percentage of `clipDuration`.
 *   Values greater than `1.0` or less than `0.0` are invalid.
 *   Using a small value will give more accurate quantile computations
 *   with the tradeoff of more memory used.
 *
 * @param history how long to hold onto data for
 *
 * @param slices the number of slices to use. See util's `WindowedAdder`.
 *
 * @param now the current time. for testing.
 */
private[finagle] class LatencyHistogram(
  clipDuration: Long,
  error: Double,
  history: Long,
  slices: Int,
  now: () => Long
) {

  require(clipDuration.toInt > 0)
  require(error >= 0.0 && error <= 1.0, s"error must be between [0.0, 1.0], was $error")

  /** size of each "bucket" */
  private[this] val width: Int =
    if (error == 0.0) 1
    else math.max(1, (clipDuration * error).toInt)

  private[this] val lock: StampedLock = new StampedLock()

  private[this] val numBuckets: Int = (clipDuration / width).toInt + 1

  /**
   * Number of data points observed in total over the time window.
   */
  private[this] val n = WindowedAdder(history, slices, now)

  /**
   * Number of data points observed, indexed by duration, over the time window.
   */
  private[this] val tab = Array.fill(numBuckets) {
    WindowedAdder(history, slices, now)
  }

  /**
   * Compute the quantile `which` from the underlying
   * dataset using the normal algorithm without
   * interpolation.
   *
   * @param which the quantile to compute, in [0, 100)
   */
  def quantile(which: Int): Long = {
    require(which < 100 && which >= 0)

    var i = 0
    val stamp = lock.writeLock()
    try {
      // The number of samples before the request quantile.
      val target = n.sum() * which / 100 + 1
      var sum = 0L
      do {
        sum += tab(i).sum()
        i += 1
      } while (i < numBuckets && sum < target)
    } finally {
      lock.unlockWrite(stamp)
    }

    ((i - 1) * width) + (width / 2)
  }

  /**
   * Adds `d` to the histogram.
   *
   * @param d duration, which should have the same units
   * as the constructor arguments. This value is ignored if
   * its `<= 0` and will be capped at [[clipDuration]].
   */
  def add(d: Long): Unit = {
    if (d >= 0) {
      val dur: Long = math.min(d, clipDuration)
      val bucket = tab((dur / width).toInt)

      val stamp = lock.readLock()
      try {
        bucket.incr()
        n.incr()
      } finally {
        lock.unlockRead(stamp)
      }
    }
  }
}
