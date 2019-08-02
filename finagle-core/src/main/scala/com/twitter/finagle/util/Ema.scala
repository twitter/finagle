package com.twitter.finagle.util

/**
 * Maintain an exponential moving average of Double-typed values over a
 * given window on a user-defined clock.
 *
 * `Ema` requires monotonic timestamps. A monotonic
 * time source is available at `Ema.Monotime`.
 *
 * This class is NOT thread safe and requires synchronization around both
 * the timestamp generation and calls to update.
 *
 * @param window The mean lifetime of observations.
 */
private[finagle] class Ema(window: Long) {
  // as noted above, thread safety must be handled external to the class.
  private[this] var time = Long.MinValue

  // this is volatile to allow read-only calls to `last`
  // without needing synchronization.
  @volatile private[this] var ema = 0.0

  /**
   * Update the average with observed value `x`, and return the new average.
   *
   * Since `update` requires monotonic timestamps, it is up to the caller to
   * ensure that calls to update do not race.
   */
  def update(stamp: Long, x: Double): Double = {
    if (time == Long.MinValue) {
      time = stamp
      ema = x
      x
    } else {
      val td = stamp - time
      assert(td >= 0, "Nonmonotonic timestamp")
      time = stamp
      val w = if (window == 0.0) 0.0 else math.exp(-td.toDouble / window)
      val newEma = x * (1 - w) + ema * w
      ema = newEma
      newEma
    }
  }

  /**
   * Return the last observation.
   *
   * @note This is safe to call without synchronization.
   */
  def last: Double = ema

  /**
   * Reset the average to 0 and erase all observations.
   */
  def reset(): Unit = {
    time = Long.MinValue
    ema = 0.0
  }
}

private[finagle] object Ema {

  /**
   * A monotonically increasing clock.
   * The time comes from System.nanoTime(), so it's not guaranteed to be tied to
   * any specific wall clock time, and it should only be used for checking
   * elapsed time.
   *
   * Call `nanos` to sample. This class is not thread safe. Also, when used to
   * generate a timestamp for Ema#update, both the timestamp generation and
   * update must occur atomically:
   *
   * {{{
   * val ema = new Ema()
   * val monotime = new Ema.Monotime()
   * ...
   * def update(x: Long): Unit = synchronized {
   *   ema.update(monotime.nanos(), x)
   * }
   * }}}
   */
  class Monotime {
    private[this] var last = System.nanoTime()

    def nanos(): Long = {
      val sample = System.nanoTime()
      if (sample - last > 0)
        last = sample
      last
    }
  }
}
