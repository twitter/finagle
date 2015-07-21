package com.twitter.finagle.stats

import com.twitter.common.stats
import java.util

private[twitter] object BucketedHistogram {

  /**
   * Given an error, compute all the bucket values from 1 until we run out of positive
   * 32-bit ints. The error should be in percent, between 0.0 and 1.0.
   *
   * Each value in the returned array will be at most `1 + (2 * error)` larger than
   * the previous (before rounding).
   *
   * Because percentiles are then computed as the midpoint between two adjacent limits,
   * this means that a value can be at most `1 + error` percent off of the actual
   * percentile.
   *
   * The last bucket tracks up to `Int.MaxValue`.
   */
  private[this] def makeLimitsFor(error: Double): Array[Int] = {
    def build(maxValue: Double, factor: Double, n: Double): Stream[Double] = {
      val next = n * factor
      if (next >= maxValue)
        Stream.empty
      else
        Stream.cons(next, build(maxValue, factor, next))
    }
    require(error > 0.0 && error <= 1.0, error)

    val values = build(Int.MaxValue.toDouble, 1.0 + (error * 2), 1.0)
      .map(_.toInt + 1) // this ensures that the smallest value is 2 (below we prepend `1`)
      .distinct
      .force
    (Seq(1) ++ values).toArray
  }

  // 0.5% error => 1797 buckets, 7188 bytes, max 11 compares on binary search
  private[stats] val DefaultErrorPercent = 0.005

  private[this] val DefaultLimits: Array[Int] =
    makeLimitsFor(DefaultErrorPercent)

  /** check all the limits are non-negative and increasing in value. */
  private def assertLimits(limits: Array[Int]): Unit = {
    require(limits.length > 0)
    var i = 0
    var prev = -1L
    while (i < limits.length) {
      val value = limits(i)
      require(value >= 0 && value > prev, i)
      prev = value
      i += 1
    }
  }

  /**
   * Creates an instance using the default bucket limits.
   */
  def apply(): BucketedHistogram =
    new BucketedHistogram(DefaultLimits)

}


/**
 * Allows for computing approximate percentiles from a stream of
 * data points.
 *
 * The precision is relative to the size of the data points that are
 * collected (not the number of points, but how big each value is).
 *
 * For instances created using the defaults via [[BucketedHistogram.apply()]],
 * the memory footprint should be around 7.2 KB.
 *
 * This is ''not'' internally thread-safe and thread-safety must be applied
 * externally. Typically, this is done via [[MetricsBucketedHistogram]].
 *
 * ''Note:'' while the interface for [[BucketedHistogram.add(Long)]] takes a `Long`,
 * internally the maximum value we will observe is `Int.MaxValue`. This is subject
 * to change and should be considered the minimum upper bound. Also, the smallest
 * value we will record is `0`.
 *
 * ''Note:'' this code borrows heavily from
 * [[https://github.com/twitter/ostrich/blob/master/src/main/scala/com/twitter/ostrich/stats/Histogram.scala Ostrich's Histogram]].
 * A few of the differences include:
 *  - bucket limits are configurable instead of fixed
 *  - counts per bucket are stored in int's
 *  - all synchronization is external
 *  - no tracking of min, max, sum
 *
 * @param limits the values at each index represent upper bounds, exclusive,
 *               of values for the bucket. As an example, given limits of `Array(1, 3, MaxValue)`,
 *               index=0 counts values `[0..1)`,
 *               index=1 counts values `[1..3)`, and
 *               index=2 counts values `[3..MaxValue)`.
 *               An Int per bucket should suffice, as standard usage only gives
 *               20 seconds before rolling to the next BucketedHistogram.
 *               This gives you up to Int.MaxValue / 20 = ~107MM add()s per second
 *               to a ''single'' bucket.
 *
 * @see [[BucketedHistogram.apply()]] for creation.
 */
private[stats] class BucketedHistogram(
    limits: Array[Int])
  extends stats.Histogram
{
  BucketedHistogram.assertLimits(limits)

  private[this] def countsLength: Int = limits.length + 1

  /** number of samples seen per corresponding bucket in `limits` */
  private[this] val counts = new Array[Int](countsLength)

  /** total number of samples seen */
  private[this] var num = 0L

  /** total value of all samples seen */
  private[this] var total = 0L

  /**
   * Note: only values between `0` and `Int.MaxValue`, inclusive, are recorded.
   *
   * @inheritdoc
   */
  def add(value: Long): Unit = {
    val index = if (value >= Int.MaxValue) {
      total += Int.MaxValue
      countsLength - 1
    } else {
      total += value
      val asInt = value.toInt
      // recall that limits represent upper bounds, exclusive — so take the next position (+1).
      // we assume that no inputs can be larger than the largest value in the limits array.
      Math.abs(util.Arrays.binarySearch(limits, asInt) + 1)
    }
    counts(index) += 1
    num += 1
  }

  override def clear(): Unit = {
    var i = 0
    while (i < countsLength) {
      counts(i) = 0
      i += 1
    }
    num = 0
    total = 0
  }

  /**
   * Calculate the value of the percentile rank, `p`, for the added data points
   * such that `p * 100`-percent of the data points are the same or less than it.
   *
   * @param p must be within 0.0 to 1.0, inclusive.
   * @return the approximate value for the requested percentile.
   *         The returned value will be within
   *         [[BucketedHistogram.DefaultErrorPercent]] of the actual value.
   */
  def percentile(p: Double): Long = {
    if (p < 0.0 || p > 1.0)
      throw new AssertionError(s"percentile must be within 0.0 to 1.0 inclusive: $p")

    val target = Math.round(p * num)
    var total = 0L
    var i = 0
    while (i < countsLength && total < target) {
      total += counts(i)
      i += 1
    }
    i match {
      case 0 => 0
      case _ if i == countsLength => maximum
      case _ => limitMidpoint(i - 1)
    }
  }

  /**
   * The maximum value seen by calls to [[add]].
   *
   * @return 0 if no values have been added.
   *         The returned value will be within
   *         [[BucketedHistogram.DefaultErrorPercent]] of the actual value.
   */
  def maximum: Long = {
    if (num == 0) {
      0L
    } else if (counts(countsLength - 1) > 0) {
      Int.MaxValue
    } else {
      var i = countsLength - 2 // already checked the last, start 1 before
      while (i >= 0 && counts(i) == 0) {
        i -= 1
      }
      if (i == 0) 0
      else limitMidpoint(i)
    }
  }

  /**
   * The minimum value seen by calls to [[add]].
   *
   * @return 0 if no values have been added.
   *         The returned value will be within
   *         [[BucketedHistogram.DefaultErrorPercent]] of the actual value.
   */
  def minimum: Long = {
    if (num == 0) {
      0L
    } else {
      var i = 0
      while (i < countsLength && counts(i) == 0) {
        i += 1
      }
      limitMidpoint(i)
    }
  }

  /** Get the midpoint of bucket `i` */
  private[this] def limitMidpoint(i: Int): Long = {
    i match {
      case 0 => 0
      case _ if i >= limits.length => Int.MaxValue
      case _ => (limits(i - 1).toLong + limits(i)) / 2
    }
  }

  override def getQuantile(quantile: Double): Long =
    percentile(quantile)

  override def getQuantiles(quantiles: Array[Double]): Array[Long] = {
    val ps = new Array[Long](quantiles.length)
    var i = 0
    while (i < ps.length) {
      // Note: we could speed this up via just one pass over `counts` instead of
      // of a pass per quantile.
      // We could speed up calls to `percentile` by tracking the maximum
      // bucket used during `add()`s to minimize how much of `counts` to scan.
      ps(i) = percentile(quantiles(i))
      i += 1
    }
    ps
  }

  /**
   * The total of all the values seen by calls to [[add]].
   */
  def sum: Long = total

  /**
   * The number of values [[add added]].
   */
  def count: Long = num

  /**
   * The average, or arithmetic mean, of all values seen
   * by calls to [[add]].
   *
   * @return 0.0 if no values have been [[add added]].
   */
  def average: Double =
    if (num == 0) 0.0 else total/num.toDouble

}
