package com.twitter.finagle.stats

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.common.stats.{Stat => FStat, Stats => FStats}
import com.twitter.finagle.http.{HttpMuxHandler, Request, ParamMap, Response}
import com.twitter.io.Buf
import com.twitter.util.Future
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * A TwitterServer exporter for commons stats that's compatible with the old
 * AbstractApplication behavior.
 *
 * Please note that commons stats histograms rely on being sampled.  We don't
 * sample them on your behalf, so you must set up sampling.  commons stats
 * has a TimeSeriesRepositoryImpl that might be useful[0].
 *
 * However, broadly, we would discourage you from using commons stats if
 * possible.  This is intended as a tool to help you migrate away from commons
 * stats, so if you aren't already using it, please don't start!
 *
 * [0]: https://github.com/twitter/commons/blob/master/src/java/com/twitter/common/stats/TimeSeriesRepositoryImpl.java#L118
 */
class CommonsExporter extends HttpMuxHandler {

  // this pattern matches the abstract application behavior.
  val pattern = "/vars.json"

  private[this] val registry = FStats.STAT_REGISTRY

  private[this] val mapper = (new ObjectMapper()).registerModule(DefaultScalaModule)

  private[this] val writer = mapper.writer
  private[this] val prettyWriter = mapper.writer(new DefaultPrettyPrinter)

  def apply(request: Request): Future[Response] = {
    val pretty = readBooleanParam(request.params, name = "pretty")
    val content = json(pretty)
    val response = Response()
    response.content = Buf.ByteArray.Owned(content)
    Future.value(response)
  }

  private[this] def json(pretty: Boolean): Array[Byte] = {
    val stats: Seq[FStat[_ <: Number]] = registry.getStats.asScala.toSeq
    val curWriter = if (pretty) prettyWriter else writer
    curWriter.writeValueAsBytes(Map(stats.map { stat => stat.getName -> stat.read }: _*))
  }

  // copied from JsonExporter
  private[this] def readBooleanParam(
    params: ParamMap,
    name: String
  ): Boolean = {
    val vals = params.getAll(name)
    vals.nonEmpty && vals.exists { v => v == "1" || v == "true" }
  }
}
