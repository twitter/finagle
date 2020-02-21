package com.twitter.finagle.stats

import java.util

/**
 * This class serves as a compatibility layer between the old (pre-Schema) metric creation
 * interfaces of the Metrics class and the new Schema based ones. It is only used by java classes,
 * where creating the schemas inline is cumbersome and significantly hinders readability.
 *
 * @param metrics The Metrics instance which is used for the creation and storage of all metrics.
 */
@deprecated("Please use the schema interfaces on Metrics instead.", "2020-02-06")
private[stats] class MetricsViaDeprecatedInterface(metrics: Metrics) {
  val underlying: Metrics = metrics

  def getOrCreateCounter(verbosity: Verbosity, names: Seq[String]): MetricsStore.StoreCounter =
    underlying.getOrCreateCounter(
      CounterSchema(new MetricBuilder(name = names, verbosity = verbosity, statsReceiver = null)))

  def registerGauge(verbosity: Verbosity, names: Seq[String], f: => Float): Unit =
    underlying.registerGauge(
      GaugeSchema(new MetricBuilder(name = names, verbosity = verbosity, statsReceiver = null)),
      f)

  def registerLongGauge(verbosity: Verbosity, names: Seq[String], f: => Long): Unit =
    underlying.registerLongGauge(
      GaugeSchema(new MetricBuilder(name = names, verbosity = verbosity, statsReceiver = null)),
      f)

  def getOrCreateStat(verbosity: Verbosity, names: Seq[String]): MetricsStore.StoreStat =
    underlying.getOrCreateStat(
      HistogramSchema(new MetricBuilder(name = names, verbosity = verbosity, statsReceiver = null)))

  def getOrCreateStat(
    verbosity: Verbosity,
    names: Seq[String],
    percentiles: IndexedSeq[Double]
  ): MetricsStore.StoreStat =
    underlying.getOrCreateStat(
      HistogramSchema(
        new MetricBuilder(
          name = names,
          verbosity = verbosity,
          percentiles = percentiles,
          statsReceiver = null)))

  def gauges(): util.Map[String, Number] = underlying.gauges
  def counters(): util.Map[String, Number] = underlying.counters
  def histograms(): util.Map[String, Snapshot] = underlying.histograms
}
