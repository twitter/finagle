package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.util.Duration
import com.twitter.util.Time

/**
 * Utility class to handle values that expire automatically.
 *
 * ** Not thread safe **
 *
 * @param default the default value. Used as the initial value and as the value after an expiration
 * @param expiration expires after this duration
 * @param expirationCallback callback invoked when a value is expired
 * @tparam T the type of the value
 */
private[fiber_scheduler] final class ExpiringValue[T](
  default: T,
  expiration: Duration,
  expirationCallback: T => Unit = (_: T) => {}) {

  private[this] var value = default
  private[this] var expiredValue = default
  private[this] var nextExpiration = Time.Top

  /**
   * Expires the value if necessary and returns the current value.
   */
  def apply(): T = {
    if (Time.now > nextExpiration) {
      expire()
    }
    value
  }

  /**
   * Returns the expired value if present. The current value otherwise.
   */
  def getExpired(): T =
    if (nextExpiration eq Time.Top) {
      expiredValue
    } else {
      value
    }

  /**
   * Sets the value.
   * @param v the new value
   * @param refreshExpiration indicates if the expiration time should be refreshed
   */
  def set(v: T, refreshExpiration: Boolean = true): Unit = {
    if (refreshExpiration) {
      nextExpiration = expiration.fromNow
    }
    value = v
  }

  private[this] def expire(): Unit = {
    expirationCallback(value)
    expiredValue = value
    value = default
    nextExpiration = Time.Top
  }

  override def toString() = {
    val exp =
      if (nextExpiration ne Time.Top) {
        Some(nextExpiration - Time.now)
      } else {
        None
      }
    s"ExpiringValue($value, $expiredValue, $exp)"
  }
}
