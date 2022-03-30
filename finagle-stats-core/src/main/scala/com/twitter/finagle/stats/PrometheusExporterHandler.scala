package com.twitter.finagle.stats

import com.twitter.finagle.ServiceProxy
import com.twitter.finagle.http.HttpMuxHandler
import com.twitter.finagle.http.Route
import com.twitter.finagle.http.RouteIndex
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Time

/**
 *  Service loaded [[HttpMuxHandler]] that serves metrics in Prometheus format
 *
 *  This class will be service-loaded and added to the Twitter-Server admin router
 *  allowing metrics to be easily exported in Prometheus format.
 */
class PrometheusExporterHandler(exporter: PrometheusExporter)
    extends ServiceProxy(exporter)
    with HttpMuxHandler {

  def this(view: MetricsView) = this(new PrometheusExporter(view))

  def this() = this(MetricsStatsReceiver.defaultRegistry)

  private[this] val logger = Logger.get

  // protected so we can test that it's called via an override
  protected def doLog(): Unit = {
    logger.error(exporter.metricsString)
  }

  // We don't use the default export path (which is '/metrics') because we want to both add the
  // '/admin' prefix and the '/metrics' suffix has already been taken by our json metrics endpoint.
  // See https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config
  // for more info on standardized prometheus configurations.
  private val pattern = "/admin/prometheus_metrics"

  def route: Route =
    Route(
      pattern = pattern,
      handler = this,
      index = Some(
        RouteIndex(
          alias = "Prometheus Metrics",
          group = "Metrics",
          path = Some("/admin/prometheus_metrics")
        )
      )
    )

  override def close(deadline: Time): Future[Unit] = {
    val f =
      if (!logOnShutdown()) Future.Done
      else {
        FuturePool
          .unboundedPool {
            doLog()
          }.by(deadline)(DefaultTimer)
      }
    f.flatMap(_ => super.close(deadline)) //ensure parent's close is honored
  }
}
