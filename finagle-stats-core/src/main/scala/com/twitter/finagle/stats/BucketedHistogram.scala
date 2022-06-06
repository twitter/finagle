package com.twitter.finagle.stats

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicIntegerArray

private[twitter] object BucketedHistogram {

  private[stats] val DefaultQuantiles: IndexedSeq[Double] = Array(
    0.5, 0.9, 0.95, 0.99, 0.999, 0.9999
  )

  /**
   * Given an error, compute all the bucket values from 1 until we run out of positive
   * 32-bit ints. The error should be in percent, between 0.0 and 1.0.
   *
   * Each value in the returned array will be `max(1, 2 * error * n)` larger than
   * the previous `n` value (before rounding).
   *
   * Because percentiles are then computed as the midpoint between two adjacent limits,
   * this means that a value can be at most `1 + error` percent off of the actual
   * percentile.
   *
   * The last bucket tracks up to `Int.MaxValue`.
   */
  private[stats] def makeLimitsFor(error: Double): Array[Int] = {
    def build(maxValue: Double, factor: Double, n: Double): Stream[Double] = {
      val next = n * factor
      if (next >= maxValue)
        Stream.empty
      else
        Stream.cons(next, build(maxValue, factor, next))
    }
    require(error > 0.0 && error <= 1.0, error)

    // we construct an exponential bucketing system, floor every value, and then remove buckets
    // that are repeated.  in practice, this means we may or may not see duplicates until the
    // exponent gets high enough that incrementing the exponent increases the value by at least
    // one, at which point there's an inflection point.  an alternate way of constructing the same
    // array would be
    //
    // val inflectionPoint = (1.0 / (error * 2)).toInt
    // val prefix = 0.to(inflectionPoint).toSeq
    // val unadjusted = prefix ++ build(Int.MaxValue.toDouble, 1.0 + (error * 2), inflectionPoint)
    // unadjusted.map(_ + 1)
    //
    // we exploit this later on in our bucket-finding algorithm
    val values = build(Int.MaxValue.toDouble, 1.0 + (error * 2), 1.0)
      .map(_.toInt + 1) // this ensures that the smallest value is 2 (below we prepend `1`)
      .distinct
      .force
    (Seq(1) ++ values).toArray
  }

  // 0.5% error => 1797 buckets, 7188 bytes, max 11 compares on binary search
  private[stats] val DefaultErrorPercent = 0.005

  // this is exposed for testing, but should not be mutated
  private[stats] val DefaultLimits: Array[Int] =
    makeLimitsFor(DefaultErrorPercent)

  /**
   * Creates an instance using the default bucket limits.
   */
  def apply(): BucketedHistogram =
    new BucketedHistogram(DefaultErrorPercent)

  /**
   * A mutable struct used to store the most recent calculation
   * of snapshot. By reusing a single instance per Stat allows us to
   * avoid creating objects with medium length lifetimes that would
   * need to exist from one stat collection to the next.
   *
   * @param percentiles represent the quantiles that we will compute, and should
   *        be between 0 and 1.
   */
  private[stats] final class MutableSnapshot(val percentiles: IndexedSeq[Double]) {
    @volatile var count: Long = 0L
    @volatile var sum: Long = 0L
    @volatile var max: Long = 0L
    @volatile var min: Long = 0L
    @volatile var avg: Double = 0.0
    @volatile var quantiles: Array[Long] = new Array[Long](percentiles.length)

    def clear(): Unit = {
      count = 0L
      sum = 0L
      max = 0L
      min = 0L
      avg = 0.0
      quantiles = new Array[Long](percentiles.length) // resets to 0
    }
  }
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
 * This is thread-safe.
 *
 * ''Note:'' while the interface for [[add(Long)]] takes a `Long`,
 * internally the maximum value we will observe is `Int.MaxValue`. This is subject
 * to change and should be considered the minimum upper bound. Also, the smallest
 * value we will record is `0`.
 *
 * ''Note:'' this code borrows heavily from
 * [[https://github.com/twitter/ostrich/blob/master/src/main/scala/com/twitter/ostrich/stats/Histogram.scala Ostrich's Histogram]].
 * A few of the differences include:
 *  - bucket limits are configurable instead of fixed
 *  - counts per bucket are stored in int's
 *  - no tracking of min, max, sum
 *
 * @param error sets the exponential factor which controls bucket width. Higher values
 *              will result in wider buckets but less memory footprint and vise verse.
 *
 * @see [[BucketedHistogram.apply()]] for creation using the default `error` value.
 */
private[stats] final class BucketedHistogram(error: Double) {
  assert(0 < error && error < 1, "Error must be in the range (0.0, 1.0)")

  // this is the point at which we stopped seeing duplicates when we multiplied
  // the previous  number by the factor.
  // where x is the inflection point where we no longer see duplicates and y
  // is the factor
  // x + 1 > x(1 + y) => x + 1 > x + xy => 1 > xy => 1/x > y => x < 1/y
  private[this] val inflectionPoint = (1.0 / (error * 2))
  private[this] val factor = 1.0 + (error * 2)

  // we need to multiply by this to convert to the right base.
  // log_factor(x) == log10(x) / log10(factor) == log10(x) * 1 / log10(factor)
  private[this] val logFactor = 1.0 / math.log10(factor)
  private[this] def logarithm(num: Double): Double = {
    math.log10(num) * logFactor
  }

  // this is different from the floor of the inflection point because we
  // increment the floors of the powers when making the bucket limits
  private[this] val inflectionBucket = inflectionPoint.toInt + 1

  // we need to subtract the offset because our bucket making algorithm removes duplicates
  // we can find the number of buckets we would have if we hadn't removed duplicates, and
  // then subtract the number of buckets we actually have.
  private[this] val offset: Int = logarithm(inflectionBucket).toInt - inflectionBucket

  private[this] val limits =
    if (error == BucketedHistogram.DefaultErrorPercent) BucketedHistogram.DefaultLimits
    else BucketedHistogram.makeLimitsFor(error)

  /**
   * Given a number that you want to insert into a bucket, find the bucket that it should
   * go into.  If it's below the point where we're still removing duplicates, index directly
   * into the bucket. If it's above, take the logarithm.
   */
  // 0 to inflectionPoint
  private[stats] def findBucket(num: Int): Int = {
    if (num <= inflectionBucket) num
    else logarithm(num).toInt - offset
  }

  // we acquire the "read" lock when we add an element to the histogram
  // we acquire the "write" lock when we need to rezero the histogram, or when
  // we need to read the histogram.
  private[this] val sync = new NonReentrantReadWriteLock
  protected def countsLength: Int = limits.length + 1

  /** total number of samples seen */
  private[this] val num = new AtomicLong(0)

  /** total value of all samples seen */
  private[this] val total = new AtomicLong(0)

  /**
   * Number of samples seen per corresponding bucket in `limits`
   */
  private[this] val counts = new AtomicIntegerArray(countsLength)

  /**
   * Note: Only values between `0` and `Int.MaxValue`, inclusive, are recorded. Negative values
   * are treated as 0s.
   *
   * @inheritdoc
   */
  def add(value: Long): Unit = {
    var index = 0
    var truncatedValue = math.max(0, value)
    if (truncatedValue >= Int.MaxValue) {
      truncatedValue = Int.MaxValue
      index = countsLength - 1
    } else {
      index = findBucket(truncatedValue.toInt)
    }

    sync.acquireShared(1)
    try {
      total.getAndAdd(truncatedValue)
      counts.getAndIncrement(index)
      num.getAndIncrement()
    } finally {
      sync.releaseShared(1)
    }
  }

  def clear(): Unit = {
    sync.acquire(1)
    try {
      var i = 0
      while (i < countsLength) {
        counts.set(i, 0)
        i += 1
      }
      num.set(0)
      total.set(0)
    } finally {
      sync.release(1)
    }
  }

  /**
   * Calculate the value of the percentile rank, `p`, for the added data points
   * such that `p * 100`-percent of the data points are the same or less than it.
   *
   * Not threadsafe, exposed for testing.
   *
   * @param p must be within 0.0 to 1.0, inclusive.
   * @return the approximate value for the requested percentile.
   *         The returned value will be within
   *         [[BucketedHistogram.DefaultErrorPercent]] of the actual value.
   */
  def percentile(p: Double): Long = {
    if (p < 0.0 || p > 1.0)
      throw new AssertionError(s"percentile must be within 0.0 to 1.0 inclusive: $p")

    val target = Math.round(p * num.get)
    var total = 0L
    var i = 0
    while (i < countsLength && total < target) {
      total += counts.get(i)
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
   * Not threadsafe, exposed for testing.
   *
   * @return 0 if no values have been added.
   *         The returned value will be within
   *         [[BucketedHistogram.DefaultErrorPercent]] of the actual value.
   */
  def maximum: Long = {
    if (num.get == 0) {
      0L
    } else if (counts.get(countsLength - 1) > 0) {
      Int.MaxValue
    } else {
      var i = countsLength - 2 // already checked the last, start 1 before
      while (i >= 0 && counts.get(i) == 0) {
        i -= 1
      }
      if (i == 0) 0
      else limitMidpoint(i)
    }
  }

  /**
   * The minimum value seen by calls to [[add]].
   *
   * Not threadsafe, exposed for testing.
   *
   * @return 0 if no values have been added.
   *         The returned value will be within
   *         [[BucketedHistogram.DefaultErrorPercent]] of the actual value.
   */
  def minimum: Long =
    if (num.get == 0) {
      0L
    } else {
      var i = 0
      while (i < countsLength && counts.get(i) == 0) {
        i += 1
      }
      limitMidpoint(i)
    }

  /** Get the midpoint of bucket `i` */
  private[this] def limitMidpoint(i: Int): Long = {
    i match {
      case 0 => 0
      case _ if i >= limits.length => Int.MaxValue
      case _ => (limits(i - 1).toLong + limits(i)) / 2
    }
  }

  def getQuantile(quantile: Double): Long =
    percentile(quantile)

  def getQuantiles(quantiles: IndexedSeq[Double]): Array[Long] = {
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
   * Recomputes all of the metrics.  Only recompute should be used in a
   * concurrent context, since the underlying statistics methods are not
   * threadsafe and only exposed for testing.
   */
  def recompute(snap: BucketedHistogram.MutableSnapshot): Unit = {
    sync.acquire(1)
    try {
      snap.count = count
      snap.sum = sum
      snap.max = maximum
      snap.min = minimum
      snap.avg = average
      snap.quantiles = getQuantiles(snap.percentiles)
    } finally {
      sync.release(1)
    }
  }

  /**
   * The total of all the values seen by calls to [[add]].
   *
   * Not threadsafe, exposed for testing.
   */
  def sum: Long = total.get

  /**
   * The number of values [[add added]].
   *
   * Not threadsafe, exposed for testing.
   */
  def count: Long = num.get

  /**
   * The average, or arithmetic mean, of all values seen
   * by calls to [[add]].
   *
   * Not threadsafe, exposed for testing.
   *
   * @return 0.0 if no values have been [[add added]].
   */
  def average: Double = {
    val count = num.get
    if (count == 0) 0.0 else total.get / count.toDouble
  }

  /**
   * Returns a seq containing nonzero values of the histogram.
   * The sequence contains instances of BucketAndCount which are
   * the bucket's upper and lower limits and a count of the number
   * of times a value in range of the limits was added.
   */
  def bucketAndCounts: Seq[BucketAndCount] = {
    sync.acquire(1)

    // note that this method is optimized for reducing allocations, but has not
    // been benchmarked.
    try {
      // first iterate over to find the exact length of the eventual array
      var idx = 0
      var arrayLength = 0
      while (idx < countsLength) {
        if (counts.get(idx) > 0) {
          arrayLength += 1
        }
        idx += 1
      }

      // iterate again after making the array
      idx = 0
      var arrayIdx = 0
      val out = new Array[BucketAndCount](arrayLength)
      while (idx < countsLength) {
        val count = counts.get(idx)
        if (count > 0) {
          // counts is 1 bucket longer than limits
          // The last bucket of counts tracks added
          // values greater than or equal to Int.MaxValue
          val upperLimit = if (idx != limits.length) {
            limits(idx)
          } else Int.MaxValue
          val lowerLimit = if (idx != 0) {
            limits(idx - 1)
          } else 0
          out(arrayIdx) = BucketAndCount(lowerLimit, upperLimit, count)
          arrayIdx += 1
        }
        idx += 1
      }
      out
    } finally {
      sync.release(1)
    }
  }

}
