package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.exp.LatencyHistogram
import com.twitter.util.{Duration, Time, StorageUnit, Stopwatch}

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
  now: () => Long = Stopwatch.systemMillis
) {

  private[this] val histo = new LatencyHistogram(
    clipDuration = 10.seconds.inMilliseconds,
    error = 0,
    history = 1.minute.inMilliseconds,
    slices = LatencyHistogram.DefaultSlices,
    now
  )

  /**
   * Stores the [[com.twitter.util.Duration]] in a histogram unless the
   * handletime overlaps with a garbage collection.
   */
  def observe(d: Duration) {
    // discard observations that might overlap with a gc
    // TODO: do we want to buffer and then discard if there might have been a gc?
    // this has gross memory implications . . . on the other hand, this doesn't really work
    // without that.
    if (counter.lastGc < (Time.now - d))
      histo.add(d.inMilliseconds)
  }

  /**
   * Grabs the `percentile` from the handle time histogram and multiplies it
   * against `ByteCounter#rate()`, resulting in the estimated number of
   * bytes that will pass in the time it takes to handle a request.
   */
  def handleBytes(): StorageUnit = {
    lr.record("discountHistoMs", histo.quantile(percentile).toString)
    lr.record("discountRate", counter.rate().toString)

    (histo.quantile(percentile) * counter.rate()).toLong.bytes
  }
}
