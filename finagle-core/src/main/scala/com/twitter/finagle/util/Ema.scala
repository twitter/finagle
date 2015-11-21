package com.twitter.finagle.util

import com.twitter.util.Duration

/**
 * Maintain an exponential moving average of Long-typed values over a
 * given window on a user-defined clock.
 *
 * Ema requires monotonic timestamps. A monotonic
 * time source is available at [[Ema.Monotime]].
 *
 * @param window The mean lifetime of observations.
 */
private[finagle] class Ema(window: Long) {
  private[this] var time = Long.MinValue
  private[this] var ema = 0D

  def isEmpty: Boolean = synchronized { time < 0 }

  /**
   * Update the average with observed value `x`, and return the new average.
   *
   * Since `update` requires monotonic timestamps, it is up to the caller to
   * ensure that calls to update do not race.
   */
  def update(stamp: Long, x: Long): Double = synchronized {
    if (time == Long.MinValue) {
      time = stamp
      ema = x
    } else {
      val td = stamp-time
      assert(td >= 0, "Nonmonotonic timestamp")
      time = stamp
      val w = if (window == 0) 0 else math.exp(-td.toDouble/window)
      ema = x*(1-w) + ema*w
    }
    ema
  }

  /**
   * Return the last observation. This is generally only safe to use if you
   * control your own clock, since the current value depends on it.
   */
  def last: Double = synchronized { ema }

  /**
   * Reset the average to 0 and erase all observations.
   */
  def reset(): Unit = synchronized {
    time = Long.MinValue
    ema = 0
  }
}

private[finagle] object Ema {

  /**
   * A monotonically increasing clock.
   * The time comes from System.nanoTime(), so it's not guaranteed to be tied to
   * any specific wall clock time, and it should only be used for checking
   * elapsed time.
   *
   * Call `nanos` to sample.
   *
   * TODO: stops increasing if it overflows.
   */
  class Monotime {
     private[this] var last = System.nanoTime()

    def nanos(): Long = synchronized {
      val sample = System.nanoTime()
      if (sample - last > 0)
        last = sample
      last
    }

  }
}
