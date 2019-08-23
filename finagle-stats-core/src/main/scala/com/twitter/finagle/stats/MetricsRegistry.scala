package com.twitter.finagle.stats

import scala.collection.JavaConverters._
import scala.collection.mutable

private object MetricsRegistry {
  case class StatEntryImpl(delta: Double, value: Double, metricType: String) extends StatEntry

  def instantaneous(value: Double, metricType: String): StatEntry =
    StatEntryImpl(value, value, metricType)

  def cumulative(delta: Double, value: Double, metricType: String): StatEntry =
    StatEntryImpl(delta, value, metricType)
}

private[twitter] trait MetricsRegistry extends StatsRegistry {
  import MetricsRegistry._

  /**
   * A reference to the underlying Metrics representation.
   * Note, this may be null.
   */
  val registry: Metrics

  val latched: Boolean = useCounterDeltas()

  private[this] val metrics = mutable.Map.empty[String, StatEntry]

  def apply(): Map[String, StatEntry] = synchronized {
    updateMetrics()
    metrics.toMap
  }

  private[this] def updateMetrics(): Unit =
    if (registry != null) {
      for (entry <- registry.counters.entrySet.asScala) {
        val key = entry.getKey()
        val newValue = entry.getValue().doubleValue
        val newMetric = metrics.get(key) match {
          case Some(prev) => cumulative(newValue - prev.value, newValue, "counter")
          case None => cumulative(newValue, newValue, "counter")
        }
        metrics.put(key, newMetric)
      }

      for (entry <- registry.gauges.entrySet.asScala) {
        val key = entry.getKey()
        val newValue = entry.getValue().doubleValue
        metrics.put(key, instantaneous(newValue, "gauge"))
      }
    }
}
