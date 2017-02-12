package com.twitter.finagle.stats

import com.twitter.common.base.Supplier
import com.twitter.common.stats.{Percentile, Stats => FStats}
import com.twitter.util.registry.GlobalRegistry

class CommonsStatsReceiver extends StatsReceiverWithCumulativeGauges {
  GlobalRegistry.get.put(
    Seq("stats", "commons_stats", "counters_latched"),
    "false")

  val repr = FStats.STATS_PROVIDER

  @volatile private[this] var stats = Map.empty[Seq[String], Stat]
  @volatile private[this] var counters = Map.empty[Seq[String], Counter]

  override def toString: String = "CommonsStatsReceiver"

  private[this] def variableName(name: Seq[String]): String = name.mkString("_")

  protected[this] def registerGauge(name: Seq[String], f: => Float): Unit = {
    FStats.STATS_PROVIDER.makeGauge(variableName(name), new Supplier[java.lang.Float] {
      def get: java.lang.Float = new java.lang.Float(f)
    })
  }

  protected[this] def deregisterGauge(name: Seq[String]): Unit = {
    // not implemented in commons
  }

  def counter(name: String*): Counter = {
    if (!counters.contains(name)) synchronized {
      if (!counters.contains(name)) {
        val counter = new Counter {
          private[this] val underlying = FStats.exportLong(variableName(name))
          def incr(delta: Int): Unit = underlying.addAndGet(delta)
        }

        counters += (name -> counter)
      }
    }

    counters(name)
  }

  def stat(name: String*): Stat = {
    if (!stats.contains(name)) synchronized {
      if (!stats.contains(name)) {
        val stat = new Stat {
          val percentile = new Percentile[java.lang.Float](variableName(name), 100.0f , 50, 95, 99)
          def add(value: Float): Unit = percentile.record(value)
        }

        stats += (name -> stat)
      }
    }

    stats(name)
  }
}
