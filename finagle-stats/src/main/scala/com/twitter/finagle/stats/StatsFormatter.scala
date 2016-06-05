package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import scala.collection.Map
import scala.collection.mutable
import scala.util.matching.Regex

object format extends GlobalFlag[String](
  "commonsmetrics",
  "Format style for metric names (ostrich|commonsmetrics|commonsstats)"
) {
  private[stats] val Ostrich = "ostrich"
  private[stats] val CommonsMetrics = "commonsmetrics"
  private[stats] val CommonsStats = "commonsstats"
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
      case format.CommonsStats => CommonsStats
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

  /**
   * Replicates the behavior for formatting Commons Stats stats.
   *
   * See Commons Stats' `Stats.getVariables()`.
   */
  object CommonsStats extends StatsFormatter {

    private[this] def inMegabytes(l: Number): Number = l.longValue() / 1048576L
    private[this] def inSeconds(l: Number): Number = l.longValue() / 1000L
    private[this] val gcCycles: Regex = "^jvm_mem_(.*)_cycles$".r
    private[this] val gcMsec: Regex = "^jvm_mem_(.*)_msec$".r

    override def apply(values: SampledValues): Map[String, Number] = {
      val original = super.apply(values)

      original.map {
        case ("jvm_num_cpus", n) => "jvm_available_processors" -> n
        case ("jvm_classes_current_loaded", n) => "jvm_class_loaded_count" -> n
        case ("jvm_classes_total_loaded", n) => "jvm_class_total_loaded_count" -> n
        case ("jvm_classes_total_unloaded", n) => "jvm_class_unloaded_count" -> n
        case ("jvm_gc_msec", n) => "jvm_gc_collection_time_ms" -> n
        case ("jvm_gc_cycles", n) => "jvm_gc_collection_count" -> n
        case ("jvm_heap_committed", n) => "jvm_memory_heap_mb_committed" -> inMegabytes(n)
        case ("jvm_heap_max", n) => "jvm_memory_heap_mb_max" -> inMegabytes(n)
        case ("jvm_heap_used", n) => "jvm_memory_heap_mb_used" -> inMegabytes(n)
        case ("jvm_nonheap_committed", n) => "jvm_memory_non_heap_mb_committed" -> inMegabytes(n)
        case ("jvm_nonheap_max", n) => "jvm_memory_non_heap_mb_max" -> inMegabytes(n)
        case ("jvm_nonheap_used", n) => "jvm_memory_non_heap_mb_used" -> inMegabytes(n)
        case ("jvm_thread_count", n) => "jvm_threads_active" -> n
        case ("jvm_thread_daemon_count", n) => "jvm_threads_daemon" -> n
        case ("jvm_thread_peak_count", n) => "jvm_threads_peak" -> n
        case ("jvm_start_time", n) => "jvm_time_ms" -> n
        case ("jvm_uptime", n) => "jvm_uptime_secs" -> inSeconds(n)
        case (gcCycles(gc), n) => s"jvm_gc_${gc}_collection_count" -> n
        case (gcMsec(gc), n) => s"jvm_gc_${gc}_collection_time_ms" -> n
        case kv => kv
      }
    }

    protected def histoName(name: String, component: String): String =
      s"${name}_$component"

    protected def labelPercentile(p: Double): String =
      s"${p * 100}_percentile".replace(".", "_")

    protected def labelMin: String = "min"

    protected def labelMax: String = "max"

    protected def labelAverage: String = "avg"
  }
}
