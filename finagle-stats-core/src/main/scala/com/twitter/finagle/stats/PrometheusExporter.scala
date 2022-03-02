package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.MetricType

// MetricFamilySample is a class that is subject to change as additional metric
// types are supported.

/**
 * A sample of a metric. This class is subject to change as additional metric
 * types are supported.
 * @param name name of metric
 * @param metricType counter, gauge, or histogram
 * @param metricUnit the units the metric is measured in
 * @param value the value of the metric at this sample
 * @param labels key-value pair of label name and value
 */
private[stats] final case class MetricFamilySample(
  name: String,
  metricType: MetricType,
  metricUnit: Option[MetricUnit],
  value: Double,
  labels: Iterable[(String, String)])

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
  private def intToString(i: Int): String = {
    if (i == Int.MaxValue)
      "+Inf"
    else if (i == Int.MinValue)
      "-Inf"
    else
      i.toString
  }

  /**
   * Write the value of the metric as Int for counter
   */
  private def writeValueAsInt(writer: StringBuilder, value: Double): Unit = {
    writer.append(intToString(value.toInt))
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
    metricUnit: Option[MetricUnit]
  ): Unit = {
    metricUnit match {
      case Some(unit) => {
        writer.append("# UNIT ");
        writer.append(name);
        writer.append(' ')
        writer.append(unit.toString);
        writer.append('\n')
      }
      case _ =>
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

  private def writeCounter(
    writer: StringBuilder,
    name: String,
    labels: Iterable[(String, String)],
    value: Double
  ): Unit = {
    writer.append(name)
    writeLabels(writer, labels)
    writer.append(' ')
    writeValueAsInt(writer, value)
    writer.append('\n')
  }

  /**
   * Write metadata that begin with #
   */
  private def writeMetadata(
    writer: StringBuilder,
    name: String,
    metricType: MetricType,
    metricUnit: Option[MetricUnit]
  ): Unit = {
    writeType(writer, name, metricType)
    writeUnit(writer, name, metricUnit)
  }

  /**
   * Write counters to a StringBuilder
   * @param writer writer
   * @param mfs Iterable of MetricFamilySample
   */
  def writeCounters(writer: StringBuilder, mfs: Iterable[MetricFamilySample]): Unit = {
    // if true, exports metadata about the metric
    val exportMetadata: Boolean = true

    mfs.foreach { counter: MetricFamilySample =>
      if (exportMetadata) {
        writeMetadata(writer, counter.name, counter.metricType, counter.metricUnit)
      }
      writeCounter(writer, counter.name, counter.labels, counter.value)
    }
  }
}
