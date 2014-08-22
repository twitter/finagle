package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import org.jboss.netty.{util => nu}
import scala.collection.JavaConversions._
import scala.collection.mutable

private[stats] object Metric {
  def instantaneousMetric(v: Double): StatEntry =
    new StatEntry {
      val value = v
      val totalValue = v
    }

  def cumulativeMetric(v: Double, tv: Double): StatEntry =
    new StatEntry {
      val value = v
      val totalValue = tv
    }
}

private[twitter] trait MetricsRegistry extends StatsRegistry {
  val registry: Metrics
  private[this] val metrics = mutable.Map.empty[String, StatEntry]

  def getStats(): Map[String, StatEntry] = synchronized {
    updateMetrics()
    metrics.toMap
  }

  private[this] def updateMetrics(): Unit = synchronized {
    Option(registry) foreach { m =>
      for (entry <- m.sampleCounters().entrySet()) {
        val key = entry.getKey()
        val value = entry.getValue().doubleValue
        val metric = metrics.get(key) match {
          case Some(prevMetric) =>
            Metric.cumulativeMetric(value - prevMetric.totalValue, value)
          case None =>
            Metric.cumulativeMetric(value, value)
        }
        metrics.put(key, metric)
      }
      for (entry <- m.sampleGauges().entrySet()) {
        val key = entry.getKey()
        val value = entry.getValue().doubleValue
        val metric = Metric.instantaneousMetric(value)
        metrics.put(key, metric)
      }
    }
  }
}