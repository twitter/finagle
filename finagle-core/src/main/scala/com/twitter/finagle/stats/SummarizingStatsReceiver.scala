package com.twitter.finagle.stats

/**
 * Provides a `StatsReceiver` that prints nice summaries. Handy for
 * short-lived programs where you want summaries.
 */

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

import com.twitter.util.MapMaker

class SummarizingStatsReceiver extends StatsReceiverWithCumulativeGauges {
  val repr = this

  private[this] val counters = MapMaker[Seq[String], AtomicInteger] { config =>
    config.compute { _ => new AtomicInteger(0) }
  }

  // Just keep all the samples.
  private[this] val stats = MapMaker[Seq[String], ArrayBuffer[Float]] { config =>
    config.compute { _ => new ArrayBuffer[Float] }
  }

  def counter(name: String*) = new Counter {
    def incr(delta: Int) { counters(name).addAndGet(delta) }
  }

  def stat(name: String*) = new Stat {
    def add(value: Float) = { stats(name) += value }
  }

  // Ignoring gauges for now, but we may consider sampling them.
  protected[this] def registerGauge(name: Seq[String], f: => Float) {}
  protected[this] def deregisterGauge(name: Seq[String]) {}

  /* Summary */
  /* ======= */

  private[this] def variableName(name: Seq[String]) = name mkString "/"

  def summary: String = {
    val counterValues = counters map { case (k, v) => (k, v.get) }
    val statValues = stats collect {
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
