package com.twitter.finagle.stats

/**
 * Provides a `StatsReceiver` that prints nice summaries. Handy for
 * short-lived programs where you want summaries.
 */

import com.google.common.util.concurrent.AtomicLongMap
import com.google.common.cache.{CacheBuilder, CacheLoader}
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

class SummarizingStatsReceiver extends StatsReceiverWithCumulativeGauges {
  val repr = this

  private[this] val counters = AtomicLongMap.create[Seq[String]]()

  // Just keep all the samples.
  type SampleBuffer = ArrayBuffer[Float] with SynchronizedBuffer[Float]
  private[this] val stats = CacheBuilder.newBuilder()
    .build(new CacheLoader[Seq[String], SampleBuffer] {
      def load(k: Seq[String]) = new ArrayBuffer[Float] with SynchronizedBuffer[Float]
    })

  def counter(name: String*) = new Counter {
    def incr(delta: Int) { counters.addAndGet(name, delta) }
  }

  def stat(name: String*) = new Stat {
    def add(value: Float) = { stats.get(name) += value }
  }

  // Ignoring gauges for now, but we may consider sampling them.
  protected[this] def registerGauge(name: Seq[String], f: => Float) {}
  protected[this] def deregisterGauge(name: Seq[String]) {}

  /* Summary */
  /* ======= */

  private[this] def variableName(name: Seq[String]) = name mkString "/"

  def summary: String = {
    val counterValues = counters.asMap.asScala
    val statValues = stats.asMap.asScala collect {
      case (k, values) if !values.isEmpty =>
        val xs = values.sorted
        val n = xs.size
        val (min, med, max) = (xs(0), xs(n / 2), xs(n - 1))
        (k, (n, min, med, max))
    }

    val counterLines = counterValues map { case (k, v) => (variableName(k), v.toString) } toSeq
    val statLines = statValues map { case (k, (n, min, med, max)) =>
      (variableName(k), "n=%d min=%.1f med=%.1f max=%.1f".format(n, min, med, max))
    } toSeq

    val sortedCounters = counterLines.sortBy { case (k, _) => k }
    val sortedStats    = statLines.sortBy    { case (k, _) => k }

    val fmt = Function.tupled { (k: String, v: String) => "%-60s %s".format(k, v) }
    val fmtCounters = sortedCounters map fmt
    val fmtStats = sortedStats map fmt

    ("# counters\n" + (fmtCounters mkString "\n") +
     "\n# stats\n" + (fmtStats mkString "\n"))
  }

  def print() = println(summary)
}
