package com.twitter.finagle.mux.lease.exp

import com.twitter.common.quantity.{Amount, Data, Time => CommonTime}
import com.twitter.common.stats.WindowedApproxHistogram
import com.twitter.common.util.Clock
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Await, Promise, Duration, Timer, Time, StorageUnit}

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
  quantile: Double,
  lr: LogsReceiver = NullLogsReceiver,
  timer: Timer = DefaultTimer.twitter
) {
  private[this] val histo = {
    val clk = new ClockFromTimer(timer)
    val granularity = CommonTime.MILLISECONDS

    new WindowedApproxHistogram(
      Amount.of(1.minute.inMilliseconds, granularity),
      5,
      // TODO: switch to precision
      Amount.of(100.kilobytes.inBytes, Data.BYTES),
      clk
    )
  }

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
   * Grabs the `quantile` from the handle time histogram and multiplies it
   * against `ByteCounter#rate()`, resulting in the estimated number of
   * bytes that will pass in the time it takes to handle a request.
   */
  def handleBytes(): StorageUnit = {
    lr.record("discountHistoMs", histo.getQuantile(quantile).toString)
    lr.record("discountRate", counter.rate().toString)

    (histo.getQuantile(quantile) * counter.rate()).toLong.bytes
  }
}

/**
 * Makes a [[com.twitter.common.util.Clock]] from a [[com.twitter.util.Timer]] and
 * [[com.twitter.util.Time]]
 */
private[lease] class ClockFromTimer(timer: Timer) extends Clock {
  def nowMillis(): Long = {
    Time.now.inMilliseconds
  }

  def nowNanos(): Long = {
    Time.now.inNanoseconds
  }

  def waitFor(millis: Long) {
    val p = Promise[Unit]
    timer.schedule(millis.milliseconds) {
      p.setValue(())
    }
    Await.result(p)
  }
}
