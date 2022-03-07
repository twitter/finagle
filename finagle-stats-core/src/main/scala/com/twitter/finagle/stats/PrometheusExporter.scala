package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.MetricType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot

/**
 * Exports metrics in a text according to Prometheus format.
 */
private[stats] object PrometheusExporter {

  /**
   * Convert a double to its string representation with positive and negative infinity.
   */
  private def doubleToString(d: Double): String = {
    if (d == Double.MaxValue)
      "+Inf"
    else if (d == Double.MinValue)
      "-Inf"
    else
      d.toString
  }

  /**
   * Write the value of the metric as a Double
   */
  private def writeValueAsDouble(writer: StringBuilder, value: Double): Unit = {
    writer.append(doubleToString(value))
  }

  /**
   * Convert a double to its string representation with positive and negative infinity.
   */
  private def longToString(l: Long): String = {
    if (l == Long.MaxValue)
      "+Inf"
    else if (l == Long.MinValue)
      "-Inf"
    else
      l.toString
  }

  /**
   * Write the value of the metric as Long for counter
   */
  private def writeValueAsLong(writer: StringBuilder, value: Double): Unit = {
    writer.append(longToString(value.toLong))
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
   * @param writer
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
   * @param writer
   * @param labels the key-value pair of labels that the metric carries
   */
  private[stats] def writeLabels(
    writer: StringBuilder,
    labels: Iterable[(String, String)]
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
      writer.append('}')
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
  ): Unit = {
    val metricName = snapshot.builder.name.last
    if (exportMetadata) {
      writeMetadata(writer, metricName, snapshot.builder.metricType, snapshot.builder.units)
    }
    writeNameAndLabels(writer, metricName, snapshot.builder.labels)
    snapshot match {
      case gaugeSnap: GaugeSnapshot =>
        writeValueAsDouble(writer, gaugeSnap.value)
      case counterSnap: CounterSnapshot =>
        writeValueAsLong(writer, counterSnap.value)
      case _ => throw new Exception("Unsupported snapshot type")
    }
    writer.append('\n')
  }

  private def writeNameAndLabels(
    writer: StringBuilder,
    name: String,
    labels: Iterable[(String, String)]
  ): Unit = {
    writer.append(name)
    writeLabels(writer, labels)
    writer.append(' ')
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
   */
  def writeMetrics(
    writer: StringBuilder,
    counters: Iterable[CounterSnapshot],
    gauges: Iterable[GaugeSnapshot],
    exportMetadata: Boolean = true
  ): Unit = {
    counters.foreach(c => writeMetric(writer, c, exportMetadata))
    gauges.foreach(g => writeMetric(writer, g, exportMetadata))
  }
}
