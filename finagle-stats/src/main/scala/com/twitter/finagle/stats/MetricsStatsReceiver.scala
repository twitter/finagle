package com.twitter.finagle.stats

import com.twitter.common.metrics.{AbstractGauge, Metrics}
import com.twitter.finagle.http.HttpMuxHandler
import java.util.concurrent.ConcurrentHashMap

object MetricsStatsReceiver {
  val defaultRegistry = Metrics.root()
}

class MetricsStatsReceiver(val registry: Metrics) extends StatsReceiverWithCumulativeGauges {
  def this() = this(MetricsStatsReceiver.defaultRegistry)
  val repr = this

  // Use for backward compatibility with ostrich caching behavior
  private[this] val counters = new ConcurrentHashMap[Seq[String], Counter]
  private[this] val stats = new ConcurrentHashMap[Seq[String], Stat]

  /**
   * Create and register a counter inside the underlying Metrics library
   */
  def counter(names: String*): Counter = {
    var counter = counters.get(names)
    if (counter == null) counters.synchronized {
      counter = counters.get(names)
      if (counter == null) {
        counter = new Counter {
          val metricsCounter = registry.createCounter(format(names))
          def incr(delta: Int) = metricsCounter.add(delta)
        }
        counters.put(names, counter)
      }
    }
    counter
  }

  /**
   * Create and register a stat (histogram) inside the underlying Metrics library
   */
  def stat(names: String*): Stat = {
    var stat = stats.get(names)
    if (stat == null) stats.synchronized {
      stat = stats.get(names)
      if (stat == null) {
        stat = new Stat {
          val histogram = registry.createHistogram(format(names))
          def add(value: Float) = histogram.add(value.toLong)
        }
        stats.put(names, stat)
      }
    }
    stat
  }

  protected[this] def registerGauge(names: Seq[String], f: => Float) {
    val gauge = new AbstractGauge[java.lang.Double](format(names)) {
      override def read = new java.lang.Double(f)
    }
    registry.register(gauge)
  }

  protected[this] def deregisterGauge(names: Seq[String]) {
    registry.unregister(format(names))
  }

  private[this] def format(names: Seq[String]) = names.mkString("/")
}

class MetricsExporter(val registry: Metrics)
  extends JsonExporter(registry)
  with HttpMuxHandler
  with MetricsRegistry
{
  def this() = this(MetricsStatsReceiver.defaultRegistry)
  val pattern = "/admin/metrics.json"
}
