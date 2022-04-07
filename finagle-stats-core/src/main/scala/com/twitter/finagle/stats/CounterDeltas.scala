package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.UnlatchedCounter
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import java.util.Collections
import scala.collection.JavaConverters._
import java.util.{HashMap => JHashMap}

/**
 * A mechanism for obtaining delta-ed counters.
 *
 * Deltas are computed from the last `update`.
 */
private[stats] class CounterDeltas {

  /**
   * @param prev the last absolute value seen
   * @param delta the last delta computed
   */
  private class Last(val prev: CounterSnapshot, val delta: Long)

  // Last values recorded for the counters.
  @volatile private[this] var lasts = Collections.emptyMap[String, Last]

  /**
   * Return the deltas as seen by the last call to [[update]].
   */
  def deltas: Iterable[MetricsView.CounterSnapshot] = {
    lasts.values.asScala.map { l =>
      if (l.prev.builder.metricType == UnlatchedCounter) {
        l.prev
      } else {
        l.prev.copy(value = l.delta)
      }

    }
  }

  /**
   * Updates the values to be used for future calls
   * to [[deltas]].
   *
   * @param newCounters the new absolute values for the counters.
   */
  def update(newCounters: Iterable[MetricsView.CounterSnapshot]): Unit = synchronized {
    val next = new JHashMap[String, Last](newCounters.size)

    // Just grab the reference once so we don't have to suffer the volatile read every time
    val prevs = lasts
    newCounters.foreach { counter =>
      val last = prevs.get(counter.hierarchicalName)
      val current = counter.value
      val delta =
        if (last == null) current
        else current - last.prev.value

      next.put(counter.hierarchicalName, new Last(counter, delta))
    }
    lasts = next
  }
}
