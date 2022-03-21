package com.twitter.finagle.stats

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.GaugeType
import com.twitter.finagle.stats.MetricBuilder.HistogramType
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Provides a `StatsReceiver` that prints nice summaries. Handy for
 * short-lived programs where you want summaries.
 */
class SummarizingStatsReceiver extends StatsReceiverWithCumulativeGauges {

  def repr: SummarizingStatsReceiver = this

  private[this] val counters = new ConcurrentHashMap[Seq[String], AtomicLong]()

  // Just keep all the samples.
  private[this] val stats = Caffeine
    .newBuilder()
    .build(new CacheLoader[Seq[String], ArrayBuffer[Float]] {
      def load(k: Seq[String]): ArrayBuffer[Float] = new ArrayBuffer[Float]
    })

  // synchronized on `this`
  private[this] var _gauges = Map[Seq[String], () => Float]()
  def gauges: Map[Seq[String], () => Float] = synchronized { _gauges }

  def counter(metricBuilder: MetricBuilder): Counter = {
    validateMetricType(metricBuilder, CounterType)
    new Counter {
      counters.putIfAbsent(metricBuilder.name, new AtomicLong(0))
      def incr(delta: Long): Unit = counters.get(metricBuilder.name).getAndAdd(delta)
      def metadata: Metadata = metricBuilder
    }
  }

  def stat(metricBuilder: MetricBuilder): Stat = {
    validateMetricType(metricBuilder, HistogramType)
    new Stat {
      def add(value: Float): Unit = SummarizingStatsReceiver.this.synchronized {
        stats.get(metricBuilder.name) += value
      }
      def metadata: Metadata = metricBuilder
    }
  }

  override def addGauge(metricBuilder: MetricBuilder)(f: => Float): Gauge = {
    validateMetricType(metricBuilder, GaugeType)
    synchronized {
      _gauges += (metricBuilder.name -> (() => f))
      new Gauge {
        def remove(): Unit = ()
        def metadata: Metadata = metricBuilder
      }
    }
  }

  protected[this] def registerGauge(metricBuilder: MetricBuilder, f: => Float): Unit =
    synchronized {
      _gauges += (metricBuilder.name -> (() => f))
    }

  protected[this] def deregisterGauge(metricBuilder: MetricBuilder): Unit = synchronized {
    _gauges -= metricBuilder.name
  }

  /* Summary */
  /* ======= */

  private[this] def variableName(name: Seq[String]): String = name.mkString("/")

  def summary(): String = summary(false)

  def summary(includeTails: Boolean): String = synchronized {
    val counterValues = counters.asScala
    val gaugeValues = gauges.toSeq map {
      case (names, gauge) => variableName(names) -> gauge().toString
    }
    val statValues = stats.asMap.asScala.collect {
      case (k, buf) if buf.nonEmpty =>
        val n = buf.size
        val values = new Array[Float](n)
        buf.copyToArray(values, 0, n)
        val xs = values.sorted
        (k, xs)
    }

    val counterLines =
      counterValues.map {
        case (k, v) =>
          (variableName(k), v.get.toString)
      }.toSeq
    val statLines = statValues.map {
      case (k, xs) =>
        val n = xs.length
        def idx(ptile: Double) = math.floor(ptile * n).toInt
        (
          variableName(k),
          "n=%d min=%.1f med=%.1f p90=%.1f p95=%.1f p99=%.1f p999=%.1f p9999=%.1f max=%.1f".format(
            n,
            xs(0),
            xs(n / 2),
            xs(idx(.9d)),
            xs(idx(.95d)),
            xs(idx(.99d)),
            xs(idx(.999d)),
            xs(idx(.9999d)),
            xs(n - 1)
          )
        )
    }.toSeq

    lazy val tailValues = statValues.map {
      case (k, xs) =>
        val n = xs.length
        def slice(ptile: Double) = {
          val end = math.floor(ptile * n).toInt
          val start = math.ceil(end - ((1.0 - ptile) * n)).toInt
          for (i <- start to end) yield xs(i)
        }
        (variableName(k), "p999=%s, p9999=%s".format(slice(.999d), slice(.9999d)))
    }.toSeq

    val sortedCounters = counterLines.sortBy { case (k, _) => k }
    val sortedGauges = gaugeValues.sortBy { case (k, _) => k }
    val sortedStats = statLines.sortBy { case (k, _) => k }
    lazy val sortedTails = tailValues.sortBy { case (k, _) => k }

    val fmt = Function.tupled { (k: String, v: String) => "%-30s %s".format(k, v) }
    val fmtCounters = sortedCounters.map(fmt)
    val fmtGauges = sortedGauges.map(fmt)
    val fmtStats = sortedStats.map(fmt)
    lazy val fmtTails = sortedTails.map(fmt)

    "# counters\n" + fmtCounters.mkString("\n") +
      "\n# gauges\n" + fmtGauges.sorted.mkString("\n") +
      "\n# stats\n" + fmtStats.mkString("\n") +
      (if (includeTails) "\n# stats-tails\n" + fmtTails.mkString("\n") else "")
  }

  def print(): Unit = println(summary(false))
}
