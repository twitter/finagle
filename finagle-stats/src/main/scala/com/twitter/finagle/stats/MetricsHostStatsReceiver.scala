package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import com.twitter.finagle.http.HttpMuxHandler

class MetricsHostStatsReceiver(val registry: Metrics) extends HostStatsReceiver {
  def this() = this(MetricsStatsReceiver.defaultHostRegistry)

  private[this] val _self = new MetricsStatsReceiver(registry)
  def self = _self
}

class HostMetricsExporter(val registry: Metrics)
  extends JsonExporter(registry)
  with HttpMuxHandler
{
  def this() = this(MetricsStatsReceiver.defaultHostRegistry)
  val pattern = "/admin/per_host_metrics.json"
}