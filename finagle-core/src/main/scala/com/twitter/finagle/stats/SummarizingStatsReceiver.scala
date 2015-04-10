package com.twitter.finagle.stats

/**
 * Provides a `StatsReceiver` that prints nice summaries. Handy for
 * short-lived programs where you want summaries.
 */

import com.google.common.util.concurrent.AtomicLongMap
import com.google.common.cache.{CacheBuilder, CacheLoader}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class SummarizingStatsReceiver extends StatsReceiverWithCumulativeGauges {
  val repr = this

  private[this] val counters = AtomicLongMap.create[Seq[String]]()

  // Just keep all the samples.
  private[this] val stats = CacheBuilder.newBuilder()
    .build(new CacheLoader[Seq[String], ArrayBuffer[Float]] {
      def load(k: Seq[String]) = new ArrayBuffer[Float]
    })

  // synchronized on `this`
  private[this] var _gauges = Map[Seq[String], () => Float]()
  def gauges: Map[Seq[String], () => Float] = synchronized { _gauges }

  def counter(name: String*) = new Counter {
    def incr(delta: Int) { counters.addAndGet(name, delta) }
  }

  def stat(name: String*) = new Stat {
    def add(value: Float) = SummarizingStatsReceiver.this.synchronized {
      stats.get(name) += value
    }
  }

  // Ignoring gauges for now, but we may consider sampling them.
  protected[this] def registerGauge(name: Seq[String], f: => Float) = synchronized {
    _gauges += (name -> (() => f))
  }

  protected[this] def deregisterGauge(name: Seq[String]) = synchronized {
    _gauges -= name
  }

  /* Summary */
  /* ======= */

  private[this] def variableName(name: Seq[String]) = name mkString "/"

  def summary(): String = summary(false)

  def summary(includeTails: Boolean): String = synchronized {
    val counterValues = counters.asMap.asScala
    val gaugeValues = gauges.toSeq map {
      case (names, gauge) => variableName(names) -> gauge().toString
    }
    val statValues = stats.asMap.asScala collect {
      case (k, buf) if buf.nonEmpty =>
        val n = buf.size
        val values = new Array[Float](n)
        buf.copyToArray(values, 0, n)
        val xs = values.sorted
        (k, xs)
    }

    val counterLines = (counterValues map { case (k, v) => (variableName(k), v.toString) }).toSeq
    val statLines = (statValues map { case (k, xs) =>
      val n = xs.length
      def idx(ptile: Double) = math.floor(ptile*n).toInt
      (variableName(k), "n=%d min=%.1f med=%.1f p90=%.1f p95=%.1f p99=%.1f p999=%.1f p9999=%.1f max=%.1f".format(
        n, xs(0), xs(n/2), xs(idx(.9D)), xs(idx(.95D)), xs(idx(.99D)), xs(idx(.999D)), xs(idx(.9999D)), xs(n-1)))
    }).toSeq

    lazy val tailValues = (statValues map { case (k, xs) =>
      val n = xs.length
      def slice(ptile: Double) = {
        val end = math.floor(ptile*n).toInt
        val start = math.ceil(end-((1.0-ptile)*n)).toInt
        for (i <- start to end) yield xs(i)
      }
      (variableName(k), "p999=%s, p9999=%s".format(slice(.999D), slice(.9999D)))
    }).toSeq

    val sortedCounters      = counterLines.sortBy { case (k, _) => k }
    val sortedGauges        = gaugeValues.sortBy  { case (k, _) => k }
    val sortedStats         = statLines.sortBy    { case (k, _) => k }
    lazy val sortedTails    = tailValues.sortBy   { case (k, _) => k }

    val fmt = Function.tupled { (k: String, v: String) => "%-30s %s".format(k, v) }
    val fmtCounters = sortedCounters.map(fmt)
    val fmtGauges = gaugeValues.map(fmt)
    val fmtStats = sortedStats.map(fmt)
    lazy val fmtTails = sortedTails.map(fmt)

    "# counters\n" + fmtCounters.mkString("\n") +
    "\n# gauges\n" + fmtGauges.sorted.mkString("\n") +
    "\n# stats\n" + fmtStats.mkString("\n") +
    (if (includeTails) "\n# stats-tails\n" + (fmtTails mkString "\n") else "")
  }

  def print() = println(summary(false))
}
