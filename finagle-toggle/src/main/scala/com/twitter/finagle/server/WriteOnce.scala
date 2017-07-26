package com.twitter.finagle.server

import java.util.concurrent.atomic.AtomicReference

/**
 * Utility for state that can be initialized at most once.
 *
 * @param uninitialized the value to be returned from [[apply()]]
 *                      until [[write]] is called.
 */
private[finagle] class WriteOnce[T](uninitialized: T) {

  private[this] val value = new AtomicReference[T]()

  /**
   * If [[write]] has been called return that value, else return
   * [[uninitialized]].
   */
  def apply(): T = {
    val v = value.get
    if (v == null)
      uninitialized
    else
      v
  }

  /**
   * Sets the value to be returned by [[apply()]].
   *
   * If called multiple times, an `IllegalStateException` will be thrown.
   *
   * @note `null` is not an allowed value.
   */
  def write(v: T): Unit = {
    if (v == null)
      throw new IllegalArgumentException("value may not be null")
    val success = value.compareAndSet(null.asInstanceOf[T], v)
    if (!success)
      throw new IllegalStateException(s"value has already been initialized to: '${value.get}'")
  }

}
