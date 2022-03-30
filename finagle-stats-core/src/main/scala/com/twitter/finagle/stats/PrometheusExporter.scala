package com.twitter.finagle.stats

import com.twitter.finagle.Service
import com.twitter.finagle.http.MediaType
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.stats.MetricBuilder.MetricType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot
import com.twitter.util.Future

/**
 * Exports metrics in a text according to Prometheus format.
 */
private[stats] object PrometheusExporter {

  /**
   * Write the value of the metric as a Double
   */
  private def writeValueAsFloat(writer: StringBuilder, value: Float): Unit = {
    if (value == Float.MaxValue || value == Float.PositiveInfinity)
      writer.append("+Inf")
    else if (value == Float.MinValue || value == Float.NegativeInfinity)
      writer.append("-Inf")
    else
      writer.append(value)
  }

  /**
   * Write the value in the format appropriate for its dynamic type
   *
   * For gauges we need to allow for writing the value as either an integer or floating
   * point value depending on the type the gauge emits.
   */
  private def writeNumberValue(writer: StringBuilder, value: Number): Unit = value match {
    case l: java.lang.Long => writeValueAsLong(writer, l.longValue)
    case i: java.lang.Integer => writeValueAsLong(writer, i.intValue)
    case _ => writeValueAsFloat(writer, value.floatValue)
  }

  /**
   * Write the value of the metric as Long for counter
   */
  private def writeValueAsLong(writer: StringBuilder, value: Long): Unit = {
    if (value == Long.MaxValue)
      writer.append("+Inf")
    else if (value == Long.MinValue)
      writer.append("-Inf")
    else
      writer.append(value)
  }

  /**
   * Write the percentiles, count and sum for a summary
   */
  private def writeSummary(
    writer: StringBuilder,
    name: String,
    labels: Iterable[(String, String)],
    snapshot: Snapshot
  ): Unit = {
    snapshot.percentiles.foreach { p =>
      writeSummaryHistoLabels(writer, name, labels, "quantile", p.quantile)
      writeValueAsLong(writer, p.value)
      writer.append('\n')
    }
    // prometheus doesn't export min, max, average in the summary by default
    writeCounterGaugeLabels(writer, name + "_count", labels)
    writeValueAsLong(writer, snapshot.count)
    writer.append('\n')

    writeCounterGaugeLabels(writer, name + "_sum", labels)
    writeValueAsLong(writer, snapshot.sum)
  }

  /**
   * Write the type of metric. For example
   * # TYPE my_counter counter
   * @param writer StringBuilder
   * @param name metric name
   * @param metricType type of metric
   */
  private def writeType(writer: StringBuilder, name: String, metricType: MetricType): Unit = {
    writer.append("# TYPE ");
    writer.append(name);
    writer.append(' ')
    writer.append(metricType.toPrometheusString);
    writer.append('\n')
  }

  /**
   * Write the units that the metric measures in. For example
   * # UNIT my_boot_time_seconds seconds
   * @param writer StringBuilder
   * @param name metric name
   * @param metricUnit Option of unit the metric is measured in
   */
  private def writeUnit(
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
   * Write the labels, if any. For example
   * {env="prod",hostname="myhost",datacenter="sdc",region="europe",owner="frontend"}
   * @param writer StringBuilder
   * @param labels the key-value pair of labels that the metric carries
   * @param finishLabels if true, add close curly brace. false for summaries
   * and histograms because they have reserved labels that are appended last
   */
  private[stats] def writeLabels(
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
   * Write each metric
   * @param writer StringBuilder
   * @param snapshot instantaneous view of the metric
   * @param exportMetadata true if TYPE, UNIT are exported
   */
  private def writeMetric(
    writer: StringBuilder,
    snapshot: MetricsView.Snapshot,
    exportMetadata: Boolean
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
   * Write metric name and labels for counters and gauges
   * @param writer StringBuilder
   * @param name metric name
   * @param labels user-defined labels
   */
  private def writeCounterGaugeLabels(
    writer: StringBuilder,
    name: String,
    labels: Iterable[(String, String)]
  ): Unit = {
    writer.append(name)
    writeLabels(writer, labels, finishLabels = true)
    writer.append(' ')
  }

  /**
   * Write user-defined labels and reserved labels for Prometheus summary and
   * histogram
   * @param writer StringBuilder
   * @param name metric name
   * @param labels user-defined labels
   * @param reservedLabelName "quantile" or "le"
   * @param bucket the quantile of the summary or bucket of the histogram
   */
  private def writeSummaryHistoLabels(
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
   * Write metadata that begin with #
   */
  private def writeMetadata(
    writer: StringBuilder,
    name: String,
    metricType: MetricType,
    metricUnit: MetricUnit
  ): Unit = {
    writeType(writer, name, metricType)
    writeUnit(writer, name, metricUnit)
  }

  /**
   * Write metrics to a StringBuilder
   * @param writer writer
   * @param counters Iterable of CounterSnapshot
   * @param gauges Iterable of GaugeSnapshot
   * @param histograms Iterable of HistogramSnapshot
   * @param exportMetadata export the type, unit, and other metadata
   */
  def writeMetrics(
    writer: StringBuilder,
    counters: Iterable[CounterSnapshot],
    gauges: Iterable[GaugeSnapshot],
    histograms: Iterable[HistogramSnapshot],
    exportMetadata: Boolean
  ): Unit = {
    counters.foreach(c => writeMetric(writer, c, exportMetadata))
    gauges.foreach(g => writeMetric(writer, g, exportMetadata))
    histograms.foreach(h => writeMetric(writer, h, exportMetadata))
  }

  /**
   * Filter out metrics with verbosity level of DEBUG.
   * @param sample the metrics to denylist
   * @param verbose Allow debug metrics with a hierarchical name that matches
   * this pattern
   * */
  private[stats] def denylistDebugSample[A <: MetricsView.Snapshot](
    sample: Iterable[A],
    verbose: Option[String => Boolean]
  ): Iterable[A] =
    verbose match {
      case Some(pattern) =>
        sample.filter { value =>
          value.builder.verbosity != Verbosity.Debug || pattern(value.hierarchicalName)
        }

      case None =>
        sample.filter(_.builder.verbosity != Verbosity.Debug)
    }

}

/**
 * A Finagle HTTP service that exports Metrics in Prometheus format
 */
private[stats] class PrometheusExporter(metrics: MetricsView) extends Service[Request, Response] {
  self =>

  import PrometheusExporter._

  def apply(request: Request): Future[Response] = {
    val response = Response()
    // content-type version from Prometheus specs
    response.setContentType(MediaType.PlainText + "; version=0.0.4")

    val filteredCounters = denylistDebugSample[CounterSnapshot](metrics.counters, None)
    val filteredGauges = denylistDebugSample[GaugeSnapshot](metrics.gauges, None)
    val filteredHistos = denylistDebugSample[HistogramSnapshot](metrics.histograms, None)

    val writer = new StringBuilder()
    writeMetrics(writer, filteredCounters, filteredGauges, filteredHistos, exportMetadata = true)
    response.contentString = writer.toString()
    Future.value(response)
  }
}
