package com.twitter.finagle.util

import com.twitter.conversions.time._
import com.twitter.util.{Closable, Duration, Future, Time, Timer}
import org.HdrHistogram.{Histogram, Recorder}
import scala.collection.mutable

object WindowedPercentileHistogram {

  // Number of significant decimal digits to which the histogram will maintain value resolution
  // and separation. A value of 3 means +/- 1 unit at 1000.
  private val HdrPrecision = 3

  // The initial highest trackable value stored by a histogram.
  // We make sure to call `setAutoResize(true)` below to avoid the
  // `java.lang.ArrayIndexOutOfBoundsException`s that result if a value larger than
  // the highest trackable value is added.
  // If a value larger than [[MaxHighestTrackableValue]] is added, it is capped at
  // [[MaxHighestTrackableValue]].
  private val InitialHighestTrackableValue: Int = 500.millis.inMillis.toInt
  private[util] val MaxHighestTrackableValue: Int = 2.seconds.inMillis.toInt

  private def newEmptyHistogram: Histogram = {
    val h = new Histogram(InitialHighestTrackableValue, HdrPrecision)
    h.setAutoResize(true)
    h
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
    timer: Timer)
  extends Closable {
  import WindowedPercentileHistogram._

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
    currentSnapshot = histograms.fold(newEmptyHistogram)(addHistograms)
    pos = (pos + 1) % numBuckets
  }

  private[this] val flushCurrentBucketTask = timer.schedule(bucketSize)(flushCurrentBucket)

  /**
   * Add a value to the histogram.
   * @param value Value to add. Values larger than [[MaxHighestTrackableValue]] will be added
   *              as [[MaxHighestTrackableValue]].
   */
  def add(value: Int): Unit = {
    recorder.recordValue(Math.min(value, MaxHighestTrackableValue))
  }

  /**
   * Retrieve a percentile from the histogram.
   * @param percentile Percentile to retrieve. Must be be [0, 100]
   */
  def percentile(percentile: Double): Int = {
    if (percentile < 0 || percentile > 100) {
      throw new IllegalArgumentException(s"Percentile must be [0, 100]. Was: $percentile")
    }

    if (currentSnapshot != null)
      currentSnapshot.getValueAtPercentile(percentile).toInt
    else
      0
  }

  override def close(deadline: Time): Future[Unit] = {
    flushCurrentBucketTask.cancel()
    Future.Done
  }
}
