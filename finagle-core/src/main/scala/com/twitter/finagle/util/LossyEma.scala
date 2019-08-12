package com.twitter.finagle.util

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Maintain an exponential moving average, or ema, of Double-typed values over a
 * given window on a user-defined clock.
 *
 * This is an implementation of the "Application to measuring computer performance"
 * algorithm defined on the moving average Wikipedia page
 * (https://en.wikipedia.org/wiki/Moving_average).
 *
 * This class is thread-safe though at a cost. It does not record updates that
 * occur at the same time. The rationale here is that inputs that have a small
 * time delta from the prior observation do not significantly contribute much
 * to the tracked value. This comes down to the weighting, `math.exp(- timeDelta / window)`,
 * being close to 1.0 when the time delta is small. For example, let's use a window of
 * 60 seconds, or 6e7 microseconds. If an update comes in 1 microsecond after the
 * prior update, the weighting is `0.99999998` and as such the impact of that
 * new value is too small to have significant impact. Further, the `update`
 * method is fast, meaning that contention is already unlikely.
 *
 * @param window The mean lifetime of observations. Must be positive. The time unit
 *               must match `now`'s.
 *
 * @param now the "clock" used for determining when an update happens. Typically this
 *            would be a `() => Long` from `com.twitter.util.Stopwatch` which
 *            matches the time unit of `window`.
 *
 * @param initialValue The initial value used for the moving average before
 *                     receiving any `update`s.
 *
 * @see [[Ema]] for an implementation that will not drop updates at the cost
 *     of more contention.
 */
private[finagle] final class LossyEma(window: Long, now: () => Long, initialValue: Double) {

  if (window <= 0) {
    throw new IllegalArgumentException(s"window must be positive: $window")
  }

  private[this] val oneOverWindow: Double = 1.0 / window

  // `updating` is used as a lock for `lastTime` and `lastEma` as
  // these values can only be written to when `updating` is true.
  private[this] val updating = new AtomicBoolean(false)
  @volatile private[this] var lastTime = now()
  @volatile private[this] var lastEma = initialValue

  /**
   * Return the most recent moving average.
   */
  def last: Double = lastEma

  /**
   * Reset the moving average to the initial value.
   */
  def reset(): Unit = {
    // always acquire the lock for this operation. contention
    // is not something we are concerned about here.
    var updated = false
    while (!updated) {
      if (updating.compareAndSet(false, true)) {
        updated = true
        try {
          lastTime = now()
          lastEma = initialValue
        } finally {
          updating.set(false)
        }
      }
    }
  }

  /**
   * Update the moving average with `newValue`, and return the new moving average.
   *
   * Under contention, updates will be skipped and the prior moving average will
   * be returned.
   */
  def update(newValue: Double): Double = {
    if (!updating.compareAndSet(false, true)) {
      lastEma
    } else {
      try {
        val newTime = now()
        val deltaTime = newTime - lastTime
        if (deltaTime <= 0) {
          // When no time has elapsed, we do not update the value. For deltas of 0,
          // the algorithm would simply ignore the new input as math.exp(0) == 1.
          // For negative deltas, this assumes a race or clock drift and also ignores.
          lastEma
        } else {
          val weighting = 1.0 - math.exp(-deltaTime * oneOverWindow)
          val newEma = (newValue * weighting) + ((1.0 - weighting) * lastEma)
          lastEma = newEma
          lastTime = newTime
          newEma
        }
      } finally {
        updating.set(false)
      }
    }
  }
}
