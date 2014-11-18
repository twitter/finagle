package com.twitter.finagle.util

import com.twitter.util.Duration

/**
 * Maintain an exponential moving average of Long-typed values over a
 * given window on a user-defined clock.
 * 
 * @param window The mean lifetime of observations.
 */
private[finagle] class Ema(window: Long) {
  private[this] var time = -1L
  private[this] var ema = 0D

  /**
   * Update the average with observed value `x`,
   * and return the new average.
   */
  def update(stamp: Long, x: Long): Double = synchronized {
    if (time < 0) {
      time = stamp
      ema = x
    } else {
      val td = stamp-time
      time += td
      val w = if (window == 0) 0 else math.exp(-td.toDouble/window)
      ema = x*(1-w) + ema*w
    }
    ema
  }
}
