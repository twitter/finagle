package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.PercentOps._
import com.twitter.conversions.StorageUnitOps._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.util.{DefaultTimer, WindowedPercentileHistogram}
import com.twitter.util.{Duration, Stopwatch, StorageUnit, Time, Timer}

private object RequestSnooper {

  private val NumBuckets = 5

}

/**
 * RequestSnooper maintains a histogram of handle time unaffected by garbage
 * collection, and can cross-reference this histogram with the bytes per second
 * rate from a [[ByteCounter]] to create a "handle bytes" metric.
 *
 * This is useful since after our lease expires, we'll probably need to wait for
 * at minimum "handle bytes" for the outstanding requests to finish up.
 */
private[lease] class RequestSnooper(
  counter: ByteCounter,
  percentile: Int,
  lr: LogsReceiver = NullLogsReceiver,
  now: () => Long = Stopwatch.systemMillis,
  timer: Timer = DefaultTimer) {
  import RequestSnooper._

  private[this] val histogram = new WindowedPercentileHistogram(
    NumBuckets,
    1.minute / NumBuckets,
    timer
  )

  /**
   * Stores the [[com.twitter.util.Duration]] in a histogram unless the
   * handletime overlaps with a garbage collection.
   */
  def observe(d: Duration): Unit = {
    // discard observations that might overlap with a gc
    // TODO: do we want to buffer and then discard if there might have been a gc?
    // this has gross memory implications . . . on the other hand, this doesn't really work
    // without that.
    if (counter.lastGc < (Time.now - d))
      histogram.add(d.inMilliseconds.toInt)
  }

  /**
   * Grabs the `percentile` from the handle time histogram and multiplies it
   * against `ByteCounter#rate()`, resulting in the estimated number of
   * bytes that will pass in the time it takes to handle a request.
   */
  def handleBytes(): StorageUnit = {
    val p = histogram.percentile(percentile.percent)
    lr.record("discountHistoMs", p.toString)
    lr.record("discountRate", counter.rate().toString)

    (p * counter.rate()).toLong.bytes
  }
}
