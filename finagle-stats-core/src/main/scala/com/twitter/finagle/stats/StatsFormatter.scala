package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import scala.collection.Map
import scala.collection.mutable
import scala.util.matching.Regex

object format
    extends GlobalFlag[String](
      "commonsmetrics",
      "Format style for metric names (ostrich|commonsmetrics|commonsstats)"
    ) {
  private[stats] val Ostrich = "ostrich"
  private[stats] val CommonsMetrics = "commonsmetrics"
  private[stats] val CommonsStats = "commonsstats"
}

/**
 * If a histogram has no data collected (its count is 0), it can be
 * beneficial to not export all the histogram details as there will
 * be no interesting data there. When this flag is set to `false`,
 * only the `count=0` is exported. When `true`, all of the details
 * will be exported.
 */
object includeEmptyHistograms
    extends GlobalFlag[Boolean](
      false,
      "Include full histogram details when there are no data points"
    )

// When enabled, export 'short' histogram summary. Otherwise, export 'full' summary.
private object exportSlimHistogram {
  private val toggle = Toggles(toString)
  // We fall back to "disabled" when toggle isn't defined, which would be the case in our tests
  // and for the OSS users.
    .orElse(Toggle.off(toString))
  def apply(): Boolean = toggle(ServerInfo().clusterId.hashCode)
  override def toString: String = "com.twitter.finagle.stats.ExportSlimHistograms"
}

/**
 * Allows for customization of how stat names get formatted.
 */
private[twitter] sealed trait StatsFormatter {

  val histogramSeparator = "."

  def apply(values: SampledValues): Map[String, Number] = {
    val results = new mutable.HashMap[String, Number]()
    results ++= values.gauges.iterator.map { gauge => gauge.hierarchicalName -> gauge.value }
    results ++= values.counters.iterator.map { counter =>
      counter.hierarchicalName -> Long.box(counter.value)
    }

    def exportShortSummary(name: String, snapshot: Snapshot): Unit = {
      results += histoName(name, labelCount) -> snapshot.count
      results += histoName(name, labelSum) -> snapshot.sum
    }

    def exportFullSummary(name: String, snapshot: Snapshot): Unit = {
      exportShortSummary(name, snapshot)
      results += histoName(name, labelMin) -> snapshot.min
      results += histoName(name, labelMax) -> snapshot.max
      results += histoName(name, labelAverage) -> snapshot.average
    }

    values.histograms.foreach { histogram =>
      val snapshot = histogram.value
      val name = histogram.hierarchicalName

      if (snapshot.count == 0 && !includeEmptyHistograms()) {
        if (histogram.builder.histogramFormat != HistogramFormat.NoSummary) {
          results += histoName(name, labelCount) -> snapshot.count
        }
      } else {
        histogram.builder.histogramFormat match {
          case HistogramFormat.Default =>
            if (exportSlimHistogram()) exportShortSummary(name, snapshot)
            else exportFullSummary(name, snapshot)
          case HistogramFormat.ShortSummary =>
            exportShortSummary(name, snapshot)
          case HistogramFormat.FullSummary =>
            exportFullSummary(name, snapshot)
          case HistogramFormat.NoSummary =>
          // DO NOTHING
        }

        for (p <- snapshot.percentiles) {
          val percentileName = histoName(name, labelPercentile(p.quantile))
          results += percentileName -> p.value
        }
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
  private[twitter] final def histoName(name: String, component: String): String =
    s"${name}$histogramSeparator$component"

  /** Label applied for the number of times a histogram was reported */
  private[twitter] final val labelCount: String = HistogramFormatter.labelCount

  /** Label applied for sum of the reported values */
  private[twitter] final val labelSum: String = HistogramFormatter.labelSum

  /** Label applied for a given percentile, `p`, of a histogram */
  private[twitter] def labelPercentile(p: Double): String

  /** Label applied for the minimum of a histogram */
  private[twitter] def labelMin: String

  /** Label applied for the maximum of a histogram */
  private[twitter] def labelMax: String

  /** Label applied for the average of a histogram */
  private[twitter] def labelAverage: String
}

private[twitter] object StatsFormatter {

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
    def labelPercentile(p: Double): String = HistogramFormatter.labelPercentile(p)
    def labelMin: String = HistogramFormatter.labelMin
    def labelMax: String = HistogramFormatter.labelMax
    def labelAverage: String = HistogramFormatter.labelAverage
  }

  /**
   * Replicates the behavior for formatting Ostrich stats.
   *
   * See Ostrich's `Distribution.toMap`.
   */
  object Ostrich extends StatsFormatter {
    def labelPercentile(p: Double): String = {
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

    def labelMin: String = "minimum"

    def labelMax: String = "maximum"

    def labelAverage: String = "average"
  }

  /**
   * Replicates the behavior for formatting Commons Stats stats.
   *
   * See Commons Stats' `Stats.getVariables()`.
   */
  object CommonsStats extends StatsFormatter {

    private[this] def inMegabytes(l: Number): Number = l.longValue() / 1048576L
    private[this] def inSeconds(l: Number): Number = l.longValue() / 1000L
    private[this] val gcCycles: Regex = "^jvm_gc_(.*)_cycles$".r
    private[this] val gcMsec: Regex = "^jvm_gc_(.*)_msec$".r

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

    override val histogramSeparator: String = "_"

    def labelPercentile(p: Double): String =
      s"${p * 100}_percentile".replace(".", "_")

    def labelMin: String = "min"

    def labelMax: String = "max"

    def labelAverage: String = "avg"
  }
}
