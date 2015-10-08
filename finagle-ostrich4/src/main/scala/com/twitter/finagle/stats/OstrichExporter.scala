package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.http.{RequestParamMap, Response, Request, HttpMuxHandler}
import com.twitter.io.Buf
import com.twitter.ostrich.stats.{StatsListener, Stats}
import com.twitter.util.Future
import com.twitter.util.registry.GlobalRegistry

object ostrichFilterRegex extends GlobalFlag(Seq.empty[String], "Ostrich filter regex")

class OstrichExporter extends HttpMuxHandler {
  val pattern = "/stats.json"

  val regexes = ostrichFilterRegex().toList.map(_.r)

  GlobalRegistry.get.put(
    Seq("stats", "ostrich", "counters_latched"),
    "true")

  def apply(request: Request): Future[Response] = {
    val params = new RequestParamMap(request)
    val period = params.get("period")
    val namespace = params.get("namespace")
    val filtered = params.get("filtered").getOrElse("0") == "1"

    val content = json(period, namespace, filtered)
    val response = Response()
    response.content = Buf.Utf8(content)
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
