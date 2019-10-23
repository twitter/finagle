package com.twitter.finagle.util

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.BucketAndCount
import com.twitter.util.{Closable, Duration, Future, MockTimer, Time, Timer}
import org.HdrHistogram.{Histogram, HistogramIterationValue, Recorder}
import scala.collection.mutable

object WindowedPercentileHistogram {
  // Based on testing, a window of 30 seconds and 3 buckets tracked request
  // latency well and had no noticeable performance difference vs. a greater number of
  // buckets.
  private[finagle] val DefaultNumBuckets: Int = 3
  private[finagle] val DefaultBucketSize: Duration = 10.seconds
  private[finagle] val DefaultLowestDiscernibleValue: Int = 1
  private[finagle] val DefaultHighestTrackableValue: Int = 2.seconds.inMillis.toInt

  // Number of significant decimal digits to which the histogram will maintain value resolution
  // and separation. A value of 3 means +/- 1 unit at 1000.
  private val HdrPrecision = 3

  private def newEmptyHistogram(
    lowestDiscernibleValue: Int,
    highestTrackableValue: Int
  ): Histogram = {
    new Histogram(lowestDiscernibleValue, highestTrackableValue, HdrPrecision)
  }

  private val addHistograms: (Histogram, Histogram) => Histogram =
    (h1: Histogram, h2: Histogram) => {
      h1.add(h2)
      h1
    }
}

/**
 * Sliding window of `numBuckets` histograms. Each bucket covers a `bucketSize` interval and must
 * be full before being counted in `percentile`.
 */
class WindowedPercentileHistogram(
  numBuckets: Int,
  bucketSize: Duration,
  lowestDiscernibleValue: Int,
  val highestTrackableValue: Int,
  timer: Timer)
    extends Closable {
  import WindowedPercentileHistogram._

  def this(numBuckets: Int, bucketSize: Duration, timer: Timer) =
    this(
      numBuckets,
      bucketSize,
      WindowedPercentileHistogram.DefaultLowestDiscernibleValue,
      WindowedPercentileHistogram.DefaultHighestTrackableValue,
      timer
    )

  def this(timer: Timer) =
    this(
      WindowedPercentileHistogram.DefaultNumBuckets,
      WindowedPercentileHistogram.DefaultBucketSize,
      WindowedPercentileHistogram.DefaultLowestDiscernibleValue,
      WindowedPercentileHistogram.DefaultHighestTrackableValue,
      timer
    )

  // Provides stable interval Histogram samples from recorded values without
  // stalling recording. `recordValue` can be called concurrently.
  private[this] val recorder = new Recorder(HdrPrecision)

  // Circular buffer of [[Histogram]]s. `recorder.getIntervalHistogram` optionally takes a
  // [[Histogram]] to re-use, but it must have been produced from a previous call to
  // `recorder.getIntervalHistogram`, so we populate the buffer as such.
  // Writes/reads only occur in the synchronized `flushCurrentBucket` method.
  private[this] val histograms =
    mutable.Seq.fill[Histogram](numBuckets)(recorder.getIntervalHistogram)

  @volatile private[this] var currentSnapshot: Histogram = null

  // Current index in the buffer to flush to. Writes/reads only occur in the synchronized
  // `flushCurrentBucket` method.
  private[this] var pos: Int = 0

  // exposed for testing.
  private[util] def flushCurrentBucket(): Unit = synchronized {
    histograms(pos) = recorder.getIntervalHistogram(histograms(pos))
    currentSnapshot = histograms.fold(
      newEmptyHistogram(lowestDiscernibleValue, highestTrackableValue)
    )(addHistograms)
    pos = (pos + 1) % numBuckets
  }

  private[this] val flushCurrentBucketTask = timer.schedule(bucketSize)(flushCurrentBucket)

  /**
   * Add a value to the histogram.
   * @param value Value to add. Values larger than [[highestTrackableValue]] will be added
   *              as [[highestTrackableValue]].
   */
  def add(value: Int): Unit = {
    recorder.recordValue(Math.min(value, highestTrackableValue))
  }

  /**
   * Retrieve a percentile from the histogram.
   * @param percentile Percentile to retrieve. Must be be [0.0, 1.0]
   */
  def percentile(percentile: Double): Int = {
    if (percentile < 0 || percentile > 1) {
      throw new IllegalArgumentException(s"Percentile must be [0.0, 1.0]. Was: $percentile")
    }

    if (currentSnapshot != null)
      currentSnapshot.getValueAtPercentile(percentile * 100).toInt
    else
      0
  }

  override def close(deadline: Time): Future[Unit] = {
    flushCurrentBucketTask.cancel()
    Future.Done
  }

  /**
   * Convert the histogram to BucketAndCounts.
   *
   * For information on how the bucketing works in HDR histograms, see
   * [[com.twitter.finagle.util.WindowedPercentileHistogram.calculateBucketForValue()]].
   */
  private[finagle] def toBucketAndCounts(): Seq[BucketAndCount] = {

    def createBucketAndCounts(
      histogramIterator: java.util.Iterator[HistogramIterationValue]
    ): Seq[BucketAndCount] = {
      var bucketAndCounts = mutable.ArrayBuffer[BucketAndCount]()

      while (histogramIterator.hasNext) {
        val bucket = histogramIterator.next()
        if (bucket.getCountAtValueIteratedTo != 0) {
          // Special case where there is a count for value 0. In this case, the iterator returns
          // 0 for the valueIteratedTo and 0 for the valueIteratedFrom so we need to ensure the
          // bucket is accurate as a BucketAndCount(0, 1, count)
          if (bucket.getValueIteratedTo == 0)
            // bucket.valueIteratedTo is the inclusive highest value of the interval.
            // bucket.valueIteratedFrom is the inclusive highest value of the
            // interval previously visited.
            bucketAndCounts += BucketAndCount(
              bucket.getValueIteratedFrom,
              bucket.getValueIteratedTo + 1,
              bucket.getCountAtValueIteratedTo.toInt)
          else
            // In order to translate the hdr histogram to BucketAndCounts, we add 1 to the
            // valueIteratedFrom and the valueIteratedTo because hdr histogram's interval is
            // (min, max] whereas BucketAndCount's is [min, max).
            bucketAndCounts += BucketAndCount(
              bucket.getValueIteratedFrom + 1,
              bucket.getValueIteratedTo + 1,
              bucket.getCountAtValueIteratedTo.toInt)
        }
      }
      // toSeq required for 2.13 compat
      bucketAndCounts.toSeq
    }

    val iterator = synchronized {
      if (currentSnapshot != null)
        // We use allValues() here instead of recordedValues() because recordedValues() only
        // iterates to values that have a count greater than 0, therefore we lose data about the
        // buckets if the values are not contiguous. All the data is still there, it's
        // just the buckets are bad.
        currentSnapshot.allValues().iterator()
      else null
    }

    if (iterator != null)
      createBucketAndCounts(iterator)
    else Nil
  }

  /**
   * Given a value, calculate the BucketAndCount interval for it.
   *
   * Of course, if the HDR histogram implementation changes, this will likely also need to change.
   *
   * The bucket's size is partially determined by the significantDigits. First, the largest value
   * that has single unit resolution is determined by the following formula:
   * 2 * 10^significantDigits. Then the bucket's size is calculated as a power-of-two which
   * contains the largest value with single unit resolution (which was determined by the number of
   * significant digits we care about). The first bucket's size we call C. For example, if the
   * number of significant digits is set to 3 the first bucket's size will be 2048 (i.e. C = 2048)
   * because 2048 is the lowest power-of-two that contains 2000, calculated by 2 * 10^3. See
   * https://github.com/HdrHistogram/HdrHistogram/blob/master/src/main/java/org/HdrHistogram/AbstractHistogram.java#L306-L317.
   * For this example, within this first bucket of values 0 to 2047, each value will be exact.
   * This translates to bucket sizes of 1 for values from 0 to 2047 in BucketAndCounts. Beyond
   * 2047, the buckets' precision decreases. Subsequent buckets' sises are calculated using the
   * formula: C * 2^bucketIndex, where precision = 2^bucketIndex.
   * Note: the first bucket has an index of 0.
   *
   * For the buckets that have a bucketIndex greater than 0, there is some overlap between the
   * buckets but the lower indexed buckets are used because it has higher precision.
   *
   * There is an example using a 3 as the number of significant digits:
   * https://github.com/HdrHistogram/HdrHistogram/blob/master/src/main/java/org/HdrHistogram/AbstractHistogram.java#L345-L360
   */
  private[util] def calculateBucketForValue(
    value: Int,
    count: Int,
    significantDigits: Int
  ): BucketAndCount = {
    // Calculate the largest value with single unit resolution. This means, the largest value
    // that absolutely must be precise. For example, if the number of significant digits is 3, we
    // expect a three decimal point accuracy; therefore +/- 1 for 1000. See the HDR histogram
    // docs for a more detailed explanation:
    // https://github.com/HdrHistogram/HdrHistogram/blob/master/src/main/java/org/HdrHistogram/AbstractHistogram.java#L306-L308
    // Determined by the formula: 2 * 10^significantDigits
    val largestValueWithSingleUnitResolution = 2 * Math.pow(10, significantDigits)
    // This is the HDR histogram's first bucket's size. This bucket has a resolution of 1. For
    // example, if we add values 1 though 4 to this first bucket that has a size of 32, value 1
    // through 4 will each have a count of 1. The first bucket has a bucketIndex of 0.
    val bucketWithOneUnitResolution =
      Math.pow(2, Math.ceil(Math.log(largestValueWithSingleUnitResolution) / Math.log(2)))

    // Helper method that figures out if we are at the beginning of the next bucket.
    // C * 2^bucketIndex
    def isBeginningOfBucket(i: Double): Boolean =
      Math.ceil(Math.log(i) / Math.log(2)) == Math.floor(Math.log(i) / Math.log(2))

    // Check if the value is in the bucket with bucketIndex = 0.
    if (value < bucketWithOneUnitResolution)
      BucketAndCount(value, value + 1, count)
    else {
      // In bucket with bucketIndex > 0. This means less precision.
      val indexOfValueOutsideOfFirstBucket = value / bucketWithOneUnitResolution
      if (isBeginningOfBucket(indexOfValueOutsideOfFirstBucket)) {
        // Value is the first value of the bucket at bucketIndex = bucketNumber.
        val bucketNumber = Math.ceil(Math.log(indexOfValueOutsideOfFirstBucket) / Math.log(2)) + 1
        // 2^bucketNumber = precision, so value + precision is the interval.
        BucketAndCount(value, value + Math.pow(2, bucketNumber).toLong, count)
      } else {
        // We find the precision through the bucket index, so we must
        // calculate the bucket the value is in.
        val bucketNumberOfThisValue =
          Math.ceil(Math.log(indexOfValueOutsideOfFirstBucket) / Math.log(2))
        // This is the size of the interval.
        val precisionForThisBucket = Math.pow(2, bucketNumberOfThisValue).toInt
        // Calculate where in the interval this value lies.
        val valueModPrecision = value % precisionForThisBucket

        if (valueModPrecision == 0)
          // Value is the start of the interval.
          BucketAndCount(value, value + precisionForThisBucket, count)
        else {
          // Value is somewhere in the interval so we need to find the interval's start.
          val startOfInterval = value - valueModPrecision
          BucketAndCount(startOfInterval, startOfInterval + precisionForThisBucket, count)
        }
      }
    }
  }
}

/**
 * Just for testing.  Stores only the last added value
 */
private[finagle] class MockWindowedPercentileHistogram(timer: MockTimer)
    extends WindowedPercentileHistogram(0, Duration.Top, 1, 2000, timer) {

  def this() = this(new MockTimer())

  private[this] var _value: Int = 0

  var closed = false

  override def add(value: Int): Unit =
    _value = value

  override def percentile(percentile: Double): Int =
    _value

  override def close(deadline: Time): Future[Unit] = {
    closed = true
    Future.Done
  }
}
