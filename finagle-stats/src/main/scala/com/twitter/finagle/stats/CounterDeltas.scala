package com.twitter.finagle.stats

import java.util.{Collections, Map => JMap, HashMap => JHashMap}
import scala.collection.JavaConverters._

/**
 * A mechanism for obtaining delta-ed counters.
 *
 * Deltas are computed from the last `update`.
 */
private[stats] class CounterDeltas {

  /**
   * @param abs the last absolute value seen
   * @param delta the last delta computed
   */
  private class Last(val abs: Long, val delta: Long)

  /**
   * Last values recorded for the counters.
   *
   * thread safety provided by synchronization on `this`
   */
  private[this] var lasts = Collections.emptyMap[String, Last]

  /**
   * Return the deltas as seen by the last call to [[update]].
   */
  def deltas: Map[String, Number] = {
    val prevs = synchronized(lasts)
    prevs.asScala.map {
      case (key, pd) =>
        key -> Long.box(pd.delta)
    }.toMap
  }

  /**
   * Updates the values to be used for future calls
   * to [[deltas]].
   *
   * @param newCounters the new absolute values for the counters.
   */
  def update(newCounters: JMap[String, Number]): Unit = synchronized {
    val next = new JHashMap[String, Last](newCounters.size)
    newCounters.asScala.foreach {
      case (k, v) =>
        val last = lasts.get(k)
        val current = v.longValue
        val delta =
          if (last == null) current
          else {
            current - last.abs
          }
        next.put(k, new Last(current, delta))
    }
    lasts = next
  }

}
