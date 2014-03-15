package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.ostrich.stats.{StatsListener, Stats}
import com.twitter.finagle.http.HttpMuxHandler
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

object ostrichFilterRegex extends GlobalFlag(Seq.empty[String], "Ostrich filter regex")

class OstrichExporter extends HttpMuxHandler {
  val pattern = "/stats.json"

  val regexes = ostrichFilterRegex().toList map { _.r }

  def apply(request: HttpRequest): Future[HttpResponse] = {
    val params = new QueryStringDecoder(request.getUri).getParameters
    val period = Option(params.get("period")) map { _.get(0) }
    val namespace = Option(params.get("namespace")) map { _.get(0) }
    val filtered = Option(params.get("filtered")).map({ _.get(0) }).getOrElse("0") == "1"

    val content = json(period, namespace, filtered)
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    response.setContent(ChannelBuffers.wrappedBuffer(content.getBytes))
    Future.value(response)
  }

  def json(period: Option[String], namespace: Option[String], filtered: Boolean): String = {

    // TODO: read command line args (minPeriod, filterRegex)?
    val summary = (period, namespace) match {
      case (Some(period), _) =>
        val duration = period.toInt.seconds
        StatsListener(duration, Stats, regexes).get(filtered)
      case (None, Some(namespace)) =>
        StatsListener(namespace, Stats).get(filtered)
      case _ =>
        Stats.get()
    }

    summary.toJson
  }
}
