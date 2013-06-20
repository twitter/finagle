package com.twitter.finagle.stats

import com.twitter.common.metrics.{AbstractGauge, Histogram, Metrics}
import com.twitter.finagle.http.HttpMuxHandler
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

object MetricsStatsReceiver {
  val defaultRegistry = Metrics.root()
}

class MetricsStatsReceiver(val registry: Metrics) extends StatsReceiver {
  def this() = this(MetricsStatsReceiver.defaultRegistry)

  val repr = this
  private[this] val counters = new ConcurrentHashMap[Seq[String], Counter]
  private[this] val stats = new ConcurrentHashMap[Seq[String], Stat]
  private[this] val gauges = mutable.Map.empty[Seq[String], Gauge]

  private[this] def getOrElse[T](
    map: ConcurrentHashMap[Seq[String], T],
    names: Seq[String],
    default: => T
  ) = {
    val instance = map.get(names)
    if (instance != null) instance else map.synchronized {
      if (map.get(names) == null)
        map.put(names, default)
      map.get(names)
    }
  }

  def counter(names: String*): Counter =
    getOrElse(counters, names, new Counter {
      val metricsCounter = registry.registerCounter(format(names))
      def incr(delta: Int) = metricsCounter.add(delta)
    })

  /**
   * Create a Stat with the description
   */
  def stat(names: String*): Stat =
    getOrElse(stats, names, new Stat {
      val histogram = new Histogram(format(names), registry)
      def add(value: Float) {
        histogram.add(value.toLong)
      }
    })

  /**
   * Register a function to be periodically measured. This measurement
   * exists in perpetuity. If you call addGauge with the name of a gauge
   * previously registered, it will just return the previous gauge.
   */
  def addGauge(names: String*)(f: => Float): Gauge = synchronized {
    gauges.getOrElseUpdate(names, {
      val gauge = new AbstractGauge[java.lang.Float](format(names)) {
        override def read = new java.lang.Float(f)
      }
      registry.register(gauge)
      new Gauge { def remove() {} }
    })
  }

  private[this] def format(names: Seq[String]) = names.mkString("/")
}

class MetricsExporter(registry: Metrics)
  extends JsonExporter(registry)
  with HttpMuxHandler
{
  def this() = this(MetricsStatsReceiver.defaultRegistry)
  val pattern = "/admin/metrics.json"
}
