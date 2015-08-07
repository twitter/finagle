package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import scala.collection.Map
import scala.collection.mutable

object format extends GlobalFlag[String](
  "commonsmetrics",
  "Format style for metric names (ostrich|commonsmetrics)"
) {
  private[stats] val Ostrich = "ostrich"
  private[stats] val CommonsMetrics = "commonsmetrics"
}

/**
 * Allows for customization of how stat names get formatted.
 */
private[stats] sealed trait StatsFormatter {

  def apply(values: SampledValues): Map[String, Number] = {
    val results = new mutable.HashMap[String, Number]()
    results ++= values.gauges
    results ++= values.counters

    values.histograms.foreach { case (name, snapshot) =>
      results += histoName(name, "count") -> snapshot.count
      results += histoName(name, "sum") -> snapshot.sum
      results += histoName(name, labelAverage) -> snapshot.avg
      results += histoName(name, labelMin) -> snapshot.min
      results += histoName(name, labelMax) -> snapshot.max

      for (p <- snapshot.percentiles) {
        val percentileName = histoName(name, labelPercentile(p.getQuantile))
        results += percentileName -> p.getValue
      }
    }
    results
  }

  /**
   * Returns the full formatted name of histogram.
   *
   * @param name the "name" of the histogram
   * @param component a single part of this histogram, for example the average,
   *                  count, or a percentile.
   */
  protected def histoName(name: String, component: String): String

  /** Label applied for a given percentile, `p`, of a histogram */
  protected def labelPercentile(p: Double): String

  /** Label applied for the minimum of a histogram */
  protected def labelMin: String

  /** Label applied for the maximum of a histogram */
  protected def labelMax: String

  /** Label applied for the average of a histogram */
  protected def labelAverage: String
}

private[stats] object StatsFormatter {

  /**
   * Uses the global flag, [[format]], to select the formatter used.
   */
  def default: StatsFormatter =
    format() match {
      case format.Ostrich => Ostrich
      case format.CommonsMetrics => CommonsMetrics
    }

  /**
   * The default behavior for formatting as done by Commons Metrics.
   *
   * See Commons Metrics' `Metrics.sample()`.
   */
  object CommonsMetrics extends StatsFormatter {
    protected def histoName(name: String, component: String): String =
      s"$name.$component"

    protected def labelPercentile(p: Double): String = {
      // this has a strange quirk that p999 gets formatted as p9990
      val gname: String = "p" + (p * 10000).toInt
      if (3 < gname.length && ("00" == gname.substring(3))) {
        gname.substring(0, 3)
      } else {
        gname
      }
    }

    protected def labelMin: String = "min"

    protected def labelMax: String = "max"

    protected def labelAverage: String = "avg"
  }

  /**
   * Replicates the behavior for formatting Ostrich stats.
   *
   * See Ostrich's `Distribution.toMap`.
   */
  object Ostrich extends StatsFormatter {
    protected def histoName(name: String, component: String): String =
      s"$name.$component"

    protected def labelPercentile(p: Double): String = {
      p match {
        case 0.5d => "p50"
        case 0.9d => "p90"
        case 0.95d => "p95"
        case 0.99d => "p99"
        case 0.999d => "p999"
        case 0.9999d => "p9999"
        case _ =>
          val padded = (p * 10000).toInt
          s"p$padded"
      }
    }

    protected def labelMin: String = "minimum"

    protected def labelMax: String = "maximum"

    protected def labelAverage: String = "average"
  }

}
