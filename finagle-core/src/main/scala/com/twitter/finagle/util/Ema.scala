package com.twitter.finagle.util

import com.twitter.util.Duration

/**
 * Maintain an exponential moving average of Long-typed values over a
 * given window on a user-defined clock.
 *
 * Ema requires nonnegative, monotonic timestamps. A monotonic
 * time source is available at [[Ema.Monotime]].
 * 
 * @param window The mean lifetime of observations.
 */
private[finagle] class Ema(window: Long) {
  private[this] var time = -1L
  private[this] var ema = 0D
  
  def isEmpty: Boolean = synchronized { time < 0 }

  /**
   * Update the average with observed value `x`,
   * and return the new average.
   */
  def update(stamp: Long, x: Long): Double = synchronized {
    assert(stamp >= 0, "Negative timestamp")

    if (time < 0) {
      time = stamp
      ema = x
    } else {
      val td = stamp-time
      assert(td >= 0, "Nonmonotonic timestamp")
      time += td
      val w = if (window == 0) 0 else math.exp(-td.toDouble/window)
      ema = x*(1-w) + ema*w
    }
    ema
  }
  
  /**
   * Return the last observation. This is generally only
   * safe to use if you control your own clock, since 
   * the current value depends on it.
   */
  def last: Double = synchronized { ema }  
}

private[finagle] object Ema {
  class Monotime {
    private[this] var last = System.nanoTime()
    
    def nanos(): Long = synchronized {
      val sample = System.nanoTime()
      if (sample > last)
        last = sample
      last
    }
  
  }
}