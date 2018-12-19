package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.DurationOps._
import com.twitter.util._
import com.twitter.util.Local.Context

private[lease] trait ByteCounter {
  def rate(): Double

  def lastGc: Time

  def info: JvmInfo
}

/**
 * WindowedByteCounter is a [[java.lang.Thread]] which sleeps for period P, and
 * after waking up, adds to an Array how many bytes were allocated while it
 * slept.  This means that the rate is windowed.  It also keeps track of the
 * last time that we did a garbage collection.
 *
 * iiii|iii|iiii|iii|iiii|iii|iiii/ii
 *
 * Every i is the thread waking up and counting the number of bytes that have
 * changed collection.  Every | is where a garbage collection is.  We also store
 * the most recent garbage collection, which is denoted by the /.  There's a
 * period P that passes between each i, so we know when a garbage collection
 * occurred precise to within period P.
 *
 * Knowing the rate is useful for guessing when our next gc will happen, and how
 * long we should wait for outstanding requests to finish draining.
 *
 * '''Note:''' You must call `start()` on this [[java.lang.Thread]] for it to begin
 * running.
 */
// It might be simpler to just make it an exponential moving average.
private[lease] class WindowedByteCounter private[lease] (val info: JvmInfo, ctx: Context)
    extends Thread("WindowedByteClock")
    with ByteCounter
    with Closable {

  import WindowedByteCounter._

  def this(info: JvmInfo) = this(info, Local.Context.empty)

  /*
   Should we be conservative wrt. count vs. usage?
   Count collections unobserved?

   Should the window size be in bytes?

   Should the "time" be monotonically increasing? With
   markers for when Gcs happen? (or rather, remaining just
   answers that..)
   */

  // Allocation observations with periodicity P; these aren't
  // necessarily contiguous observations, only the last N.
  private[this] val allocs = Array.fill(N) { StorageUnit.zero }

  private[this] def sum(): StorageUnit = {
    val _ = idx // barrier
    allocs.reduce(_ + _)
  }

  @volatile private[this] var count = info.generation()
  @volatile private[this] var idx = 0

  // possible range of gc
  /** The timestamp of the last gc, precise to within period P. */
  @volatile var lastGc: Time = Time.now
  @volatile private[this] var running = true

  // used to unflaky our tests--DO NOT USE
  @volatile private[lease] var passCount: Int = 0

  /** @return allocation rate in bytes per millisecond. */
  def rate(): Double = sum().inBytes / W.inMilliseconds
  private[this] def lastRate(): Double = allocs(idx).inBytes / P.inMilliseconds

  override def toString =
    "WindowedByteCounter(windowed=" +
      rate() + "bpms; last=" +
      lastRate() + "bpms; count=" +
      count + "; sum=" +
      sum() + "bytes)"

  /**
   * Measures the amount of bytes used since the last sample, and bumps
   * the collection number if necessary.
   */
  override def run(): Unit = {
    Local.restore(ctx)
    var prevUsed = info.used()

    while (running) {
      Time.sleep(P)

      val curUsed = info.used()
      val curCount = info.generation()
      val newlyUsed = curUsed - prevUsed

      // TODO: Wake up sleepers if the rate changes more than some
      // percentage.
      if (curCount == count && newlyUsed > StorageUnit.zero) {
        val nextIdx = (idx + 1) % N
        allocs(idx) = newlyUsed
        idx = nextIdx
      } else if (curCount == count) {
        // garbage collection race
        lastGc = Time.now
        count = info.generation()
      } else {
        // we just garbage collected, can't figure out memory rate
        lastGc = Time.now
        count = curCount
      }

      prevUsed = curUsed
      passCount += 1
    }
  }

  def close(deadline: Time): Future[Unit] = {
    running = false
    Future.Done
  }

  setDaemon(true)
}

private[lease] object WindowedByteCounter {
  // TODO: W, P could be configurable--for some servers, 100ms may be too slow
  private[lease] val W = 2000.milliseconds // window size
  private[lease] val P = 100.milliseconds // poll period
  private[lease] val N = (W.inMilliseconds / P.inMilliseconds).toInt // # of polls in a window
}
