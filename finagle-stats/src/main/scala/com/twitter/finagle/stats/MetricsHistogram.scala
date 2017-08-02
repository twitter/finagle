package com.twitter.finagle.stats

/**
 * A histogram that supports writing, querying, and resetting.
 *
 * This API is too powerful for normal usage, but is useful for implementors.
 */
trait MetricsHistogram {

  /**
   * Adds a new datapoint to the histogram.
   */
  def add(value: Long): Unit

  /**
   * Returns an immutable snapshot of the state of the histogram right now.
   *
   * If the underlying histogram is latched or buffered, data might be slow to
   * appear or might disappear after some time passes.
   *
   * To reread the histogram, snapshot must be called again.
   */
  def snapshot(): Snapshot

  /**
   * Clears all the data from the histogram.
   */
  def clear(): Unit
}
