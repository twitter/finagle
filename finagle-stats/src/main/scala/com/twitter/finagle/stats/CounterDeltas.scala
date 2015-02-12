package com.twitter.finagle.stats

import java.util.{Collections, Map => JMap}
import scala.collection.JavaConverters._

/**
 * A mechanism for obtaining delta-ed counters.
 *
 * Deltas are computed from the last `update`.
 */
private[stats] class CounterDeltas {

  /**
   * Last absolute values recorded for the counters.
   *
   * thread safety provided by synchronization on `this`
   */
  private[this] var lastCounters = Collections.emptyMap[String, Number]

  /**
   * Return the deltas as seen by the last call to `update()`.
   *
   * @param newCounters the new absolute values for the counters.
   */
  def deltas(newCounters: JMap[String, Number]): Map[String, Number] =
    computeDeltas(newCounters)

  /**
   * Updates the absolute values to be used for future calls
   * to `deltas`.
   *
   * @param newCounters the new absolute values for the counters.
   */
  def update(newCounters: JMap[String, Number]): Unit = synchronized {
    lastCounters = Collections.unmodifiableMap(newCounters)
  }

  private[this] def computeDeltas(
    newCounters: JMap[String, Number]
  ): Map[String, Number] = {
    val prevCounters = synchronized { lastCounters }
    val counters = newCounters.asScala
    var next = Map.empty[String, Number]
    counters.foreach { case (k, v) =>
      val prev = prevCounters.get(k)
      val newVal: Number = if (prev == null) v else {
        // note: this should handle overflowing Long.MaxValue fine.
        v.longValue() - prev.longValue()
      }
      next += k -> newVal
    }
    next
  }

}
