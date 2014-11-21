package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import scala.collection.JavaConversions._
import scala.collection.mutable

private object MetricsRegistry {
  case class StatEntryImpl(delta: Double, value: Double)
    extends StatEntry

  def instantaneous(value: Double): StatEntry =
    StatEntryImpl(value, value)

  def cumulative(delta: Double, value: Double): StatEntry =
    StatEntryImpl(delta, value)
}

private[twitter] trait MetricsRegistry extends StatsRegistry {
  import MetricsRegistry._

  /**
   * A reference to the underlying Metrics representation.
   * Note, this may be null.
   */
  val registry: Metrics

  private[this] val metrics = mutable.Map.empty[String, StatEntry]

  def apply(): Map[String, StatEntry] = synchronized {
    updateMetrics()
    metrics.toMap
  }

  private[this] def updateMetrics(): Unit =
    if (registry != null) {
      for (entry <- registry.sampleCounters().entrySet) {
        val key = entry.getKey()
        val newValue = entry.getValue().doubleValue
        val newMetric = metrics.get(key) match {
          case Some(prev) => cumulative(newValue - prev.value, newValue)
          case None => cumulative(newValue, newValue)
        }
        metrics.put(key, newMetric)
      }

      for (entry <- registry.sampleGauges().entrySet) {
        val key = entry.getKey()
        val newValue = entry.getValue().doubleValue
        metrics.put(key, instantaneous(newValue))
      }
    }
}
