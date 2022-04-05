package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.MetricType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot

/**
 * Exports metrics in a text according to Prometheus format.
 *
 * @param exportMetadata export the commends and metadata (lines that start with #)
 * @param exportEmptyQuantiles export the quantiles of summaries and histograms with no entries
 * @param verbosityPattern pattern used to export debug metrics based on hierarchical name
 */
private final class PrometheusExporter(
  exportMetadata: Boolean,
  exportEmptyQuantiles: Boolean,
  verbosityPattern: Option[String => Boolean]) {

  /**
   * Write the value of the metric as a `Double`.
   */
  private[this] def writeValueAsFloat(writer: StringBuilder, value: Float): Unit = {
    if (value == Float.MaxValue || value == Float.PositiveInfinity)
      writer.append("+Inf")
    else if (value == Float.MinValue || value == Float.NegativeInfinity)
      writer.append("-Inf")
    else
      writer.append(value)
  }

  /**
   * Write the value in the format appropriate for its dynamic type.
   *
   * For gauges we need to allow for writing the value as either an integer or floating
   * point value depending on the type the gauge emits.
   */
  private[this] def writeNumberValue(writer: StringBuilder, value: Number): Unit = value match {
    case l: java.lang.Long => writeValueAsLong(writer, l.longValue)
    case i: java.lang.Integer => writeValueAsLong(writer, i.intValue)
    case _ => writeValueAsFloat(writer, value.floatValue)
  }

  /**
   * Write the value of the metric as Long for counter.
   */
  private[this] def writeValueAsLong(writer: StringBuilder, value: Long): Unit = {
    if (value == Long.MaxValue)
      writer.append("+Inf")
    else if (value == Long.MinValue)
      writer.append("-Inf")
    else
      writer.append(value)
  }

  /**
   * Write the percentiles, count and sum for a summary.
   */
  private[this] def writeSummary(
    writer: StringBuilder,
    name: String,
    labels: Iterable[(String, String)],
    snapshot: Snapshot
  ): Unit = {
    if (exportEmptyQuantiles || snapshot.count > 0) {
      snapshot.percentiles.foreach { p =>
        writeSummaryHistoLabels(writer, name, labels, "quantile", p.quantile)
        writeValueAsLong(writer, p.value)
        writer.append('\n')
      }
    }

    // prometheus doesn't export min, max, average in the summary by default
    writeCounterGaugeLabels(writer, name + "_count", labels)
    writeValueAsLong(writer, snapshot.count)
    writer.append('\n')

    writeCounterGaugeLabels(writer, name + "_sum", labels)
    writeValueAsLong(writer, snapshot.sum)
  }

  /**
   * Write the type of metric.
   *
   * Example:
   *  {{{
   *    # TYPE my_counter counter
   *  }}}
   * @param writer StringBuilder
   * @param name metric name
   * @param metricType type of metric
   */
  private[this] def writeType(writer: StringBuilder, name: String, metricType: MetricType): Unit = {
    writer.append("# TYPE ");
    writer.append(name);
    writer.append(' ')
    writer.append(metricType.toPrometheusString);
    writer.append('\n')
  }

  /**
   * Write the units that the metric measures in.
   *
   * Example:
   * {{{
   *   # UNIT my_boot_time_seconds seconds
   * }}}
   *
   * @param writer StringBuilder
   * @param name metric name
   * @param metricUnit Option of unit the metric is measured in
   */
  private[this] def writeUnit(
    writer: StringBuilder,
    name: String,
    metricUnit: MetricUnit
  ): Unit = {
    def writeComment(unit: String): Unit = {
      writer.append("# UNIT ");
      writer.append(name);
      writer.append(' ')
      writer.append(unit)
      writer.append('\n')
    }
    metricUnit match {
      case Unspecified => // no units
      case CustomUnit(unit) => writeComment(unit)
      case _ => writeComment(metricUnit.toString)
    }
  }

  /**
   * Write the labels, if any.
   *
   * Example:
   * {{{
   *   {env="prod",hostname="myhost",datacenter="sdc",region="europe",owner="frontend"}
   * }}}
   *
   * @param writer StringBuilder
   * @param labels the key-value pair of labels that the metric carries
   * @param finishLabels if true, add close curly brace. false for summaries
   * and histograms because they have reserved labels that are appended last
   */
  private[this] def writeLabels(
    writer: StringBuilder,
    labels: Iterable[(String, String)],
    finishLabels: Boolean
  ): Unit = {
    if (labels.nonEmpty) {
      writer.append('{')
      var first: Boolean = true
      labels.foreach {
        case (name, value) =>
          if (first) {
            first = false
          } else {
            writer.append(',')
          }
          writer.append(name)
          writer.append('=')
          writer.append('"')
          writer.append(value)
          writer.append('"')
      }
      if (finishLabels) {
        writer.append('}')
      } else {
        writer.append(',')
      }
    }
  }

  /**
   * Write a single metric to the provided `StringBuilder`.
   *
   * @param writer StringBuilder
   * @param snapshot instantaneous view of the metric
   */
  private[this] def writeMetric(
    writer: StringBuilder,
    snapshot: MetricsView.Snapshot
  ): Unit = snapshot.builder.identity match {
    case MetricBuilder.Identity.Hierarchical(_, _) => // nop: we can't write this type.
    case MetricBuilder.Identity.Full(_, labels) =>
      val metricName = snapshot.builder.name.last
      if (exportMetadata) {
        writeMetadata(writer, metricName, snapshot.builder.metricType, snapshot.builder.units)
      }
      snapshot match {
        case gaugeSnap: GaugeSnapshot =>
          writeCounterGaugeLabels(writer, metricName, labels)
          writeNumberValue(writer, gaugeSnap.value)

        case counterSnap: CounterSnapshot =>
          writeCounterGaugeLabels(writer, metricName, labels)
          writeValueAsLong(writer, counterSnap.value)

        case summarySnap: HistogramSnapshot =>
          writeSummary(writer, metricName, labels, summarySnap.value)
      }
      writer.append('\n')
  }

  /**
   * Write metric name and labels for counters and gauges.
   *
   * @param writer StringBuilder
   * @param name metric name
   * @param labels user-defined labels
   */
  private[this] def writeCounterGaugeLabels(
    writer: StringBuilder,
    name: String,
    labels: Iterable[(String, String)]
  ): Unit = {
    writer.append(name)
    writeLabels(writer, labels, finishLabels = true)
    writer.append(' ')
  }

  /**
   * Write user-defined labels and reserved labels for Prometheus summary and histogram.
   *
   * @param writer StringBuilder
   * @param name metric name
   * @param labels user-defined labels
   * @param reservedLabelName "quantile" or "le"
   * @param bucket the quantile of the summary or bucket of the histogram
   */
  private[this] def writeSummaryHistoLabels(
    writer: StringBuilder,
    name: String,
    labels: Iterable[(String, String)],
    reservedLabelName: String,
    bucket: Double,
  ): Unit = {
    writer.append(name)
    if (labels.isEmpty) {
      writer.append('{')
    }
    writeLabels(writer, labels, finishLabels = false)
    writer.append(reservedLabelName)
    writer.append("=\"")
    writer.append(bucket)
    writer.append("\"} ")
  }

  /**
   * Write metadata that begin with a `#`.
   */
  private[this] def writeMetadata(
    writer: StringBuilder,
    name: String,
    metricType: MetricType,
    metricUnit: MetricUnit
  ): Unit = {
    writeType(writer, name, metricType)
    writeUnit(writer, name, metricUnit)
  }

  /**
   * Filter out metrics with verbosity level of `Verbosity.Debug`.
   *
   * @param sample the metrics to deny list
   * @param verbose Allow debug metrics with a hierarchical name that matches this pattern
   */
  private[this] def denylistDebugSample[A <: MetricsView.Snapshot](
    sample: Iterable[A]
  ): Iterable[A] =
    verbosityPattern match {
      case Some(pattern) =>
        sample.filter { value =>
          value.builder.verbosity != Verbosity.Debug || pattern(value.hierarchicalName)
        }

      case None =>
        sample.filter(_.builder.verbosity != Verbosity.Debug)
    }

  /**
   * Write metrics to a `StringBuilder`.
   *
   * @param writer `StringBuilder` in which to encode the metrics
   * @param metrics [[MetricsView]] to write
   */
  def writeMetrics(
    writer: StringBuilder,
    metrics: MetricsView
  ): Unit = {
    val counters = denylistDebugSample[CounterSnapshot](metrics.counters)
    val gauges = denylistDebugSample[GaugeSnapshot](metrics.gauges)
    val histograms = denylistDebugSample[HistogramSnapshot](metrics.histograms)

    counters.foreach(c => writeMetric(writer, c))
    gauges.foreach(g => writeMetric(writer, g))
    histograms.foreach(h => writeMetric(writer, h))
  }

  /**
   * Write metrics to a `String`.
   *
   * @param metrics [[MetricsView]] to write
   */
  def writeMetricsString(metrics: MetricsView): String = {
    val writer = new StringBuilder
    writeMetrics(writer, metrics)
    writer.toString()
  }
}
