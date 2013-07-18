package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.ostrich.stats.{StatsListener, Stats}
import com.twitter.finagle.http.HttpMuxHandler
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

class OstrichExporter extends HttpMuxHandler {
  val pattern = "/stats.json"

  def apply(request: HttpRequest): Future[HttpResponse] = {
    def getParam(name: String): Option[String] = Option(request.getHeader(name))

    val content = json(getParam("period"), getParam("namespace"))
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    response.setContent(ChannelBuffers.wrappedBuffer(content.getBytes))
    Future.value(response)
  }

  def json(period: Option[String], namespace: Option[String]): String = {

    // TODO: read command line args (minPeriod, filterRegex)?
    val summary = (period, namespace) match {
      case (Some(period), _) =>
        val duration = period.toInt.seconds
        StatsListener(duration, Stats).get()
      case (None, Some(namespace)) =>
        StatsListener(namespace, Stats).get()
      case _ =>
        Stats.get()
    }

    summary.toJson
  }
}
