package com.twitter.finagle.stats

import java.util.concurrent.atomic.AtomicInteger

private object StandardStatsReceiver {

  // a shared counter among an application,
  // used to create incremental unique labels for servers with all protocols
  private val serverCount = new AtomicInteger(0)

  private val standardPrefix = "standard-service-metric-v1"
  private val description = "Standard Service Metrics"
  private val rootStatsReceiver = DefaultStatsReceiver.scope(standardPrefix).scope("srv")
}

/**
 * A StatsReceiver proxy that configures all counter, stat, and gauge with:
 * 1. standard labels other than application customized labels
 * 2. isStandard attribute in Metric Metadata
 * 3. default description in Metric Metadata
 *
 * Counters are configured with [[UnlatchedCounter]].
 *
 * @note This set of metrics are expected to be exported with uniform formats and attributes,
 *       they are set with a standard prefix and neglect the [[useCounterDeltas]] and
 *       [[scopeSeparator]] flags.
 *
 * @param sourceRole Represents the "role" this service plays with respect to this metric
 * @param protocol protocol library name, e.g., thriftmux, http
 */
private[finagle] class StandardStatsReceiver protected (
  sourceRole: SourceRole,
  protocol: String,
  counter: AtomicInteger)
    extends StatsReceiverProxy {

  import StandardStatsReceiver._

  def this(sourceRole: SourceRole, protocol: String) =
    this(sourceRole, protocol, StandardStatsReceiver.serverCount)

  private[this] def withDescription(metricBuilder: MetricBuilder): MetricBuilder = {
    val existing = metricBuilder.description
    val next =
      if (existing == MetricBuilder.NoDescription) description
      else s"$description: $existing"
    metricBuilder.withDescription(next)
  }

  private[this] val serverScope = sourceRole match {
    case SourceRole.Server => s"${sourceRole.toString.toLowerCase}-${counter.getAndIncrement()}"
    case other =>
      throw new IllegalArgumentException(
        s"StandardStatsReceiver should only be applied to servers, found $other.")
  }

  final val self: StatsReceiver =
    BroadcastStatsReceiver(
      Seq(rootStatsReceiver, rootStatsReceiver.scope(protocol).scope(serverScope)))

  final override def counter(metricBuilder: MetricBuilder): Counter =
    self.counter(withDescription(metricBuilder).withStandard.withUnlatchedCounter)

  final override def stat(metricBuilder: MetricBuilder): Stat =
    self.stat(withDescription(metricBuilder).withStandard)

  final override def addGauge(metricBuilder: MetricBuilder)(f: => Float): Gauge =
    self.addGauge(withDescription(metricBuilder).withStandard)(f)
}
