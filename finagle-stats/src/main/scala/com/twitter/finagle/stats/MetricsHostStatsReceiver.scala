package com.twitter.finagle.stats

import com.twitter.common.metrics.Metrics
import com.twitter.finagle.http._
import com.twitter.finagle.loadbalancer.perHostStats
import com.twitter.io.Buf
import com.twitter.util.Future

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

  override def apply(request: Request): Future[Response] = {
    if (perHostStats()) {
      super.apply(request)
    } else {
      val response = Response()
      response.contentType = MediaType.Json
      response.content = Buf.Utf8(
        s"""{
        |  "com.twitter.finagle.loadbalancer.perHostStats": {
        |    "enabled": "false",
        |    "to enable": "run with -${perHostStats.name} and configure LoadBalancerFactory.HostStats"
        |  }
        |}""".stripMargin)
      Future.value(response)
    }
  }

}
