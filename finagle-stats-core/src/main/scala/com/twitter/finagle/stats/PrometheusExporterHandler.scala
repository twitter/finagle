package com.twitter.finagle.stats

import com.twitter.finagle.http.HttpMuxHandler
import com.twitter.finagle.http.MediaType
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
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
class PrometheusExporterHandler(metrics: MetricsView) extends HttpMuxHandler {

  def this() = this(MetricsStatsReceiver.defaultRegistry)

  private[this] val logger = Logger.get

  private[this] def verbosityPattern(req: Request): Option[String => Boolean] = {
    // We prefer the query param, tunable, flag, in that order.
    req.params
      .get("verbosity_pattern")
      .orElse(Verbose())
      .orElse(com.twitter.finagle.stats.verbose.get)
      .map(Glob.apply(_))
  }

  private[this] def exportMetadata(req: Request): Boolean =
    req.params.getBoolean("export_metadata").getOrElse(true)

  private[this] def exportEmptyQuantiles(req: Request): Boolean =
    req.params.getBoolean("export_empty_quantiles").getOrElse(includeEmptyHistograms())

  /** Render the metrics in prometheus format to a `String` */
  private[this] def metricsString(request: Request): String = {
    new PrometheusExporter(
      exportMetadata = exportMetadata(request),
      exportEmptyQuantiles = exportEmptyQuantiles(request),
      verbosityPattern = verbosityPattern(request))
      .writeMetricsString(metrics)
  }

  // protected so we can test that it's called via an override
  protected def doLog(): Unit = {
    logger.error(metricsString(Request()))
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

  def apply(request: Request): Future[Response] = {
    val response = Response()
    // content-type version from Prometheus specs
    response.setContentType(MediaType.PlainText + "; version=0.0.4")
    response.contentString = metricsString(request)
    Future.value(response)
  }
}
