package com.twitter.finagle.stats

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.app.GlobalFlag
import com.twitter.common.metrics.Metrics
import com.twitter.finagle.Service
import com.twitter.finagle.http.MediaType
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import scala.collection.immutable
import scala.util.matching.Regex

/**
 * Blacklist of regex, comma-separated. Comma is a reserved character and cannot be used.
 *
 * See http://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilter extends GlobalFlag[String]("",
                      "Comma-separated regexes that indicate which metrics to filter out")

class JsonExporter(registry: Metrics) extends Service[HttpRequest, HttpResponse] {
  private[this] val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  private[this] val writer = mapper.writer
  private[this] val prettyWriter = mapper.writer(new DefaultPrettyPrinter)

  lazy val statsFilterRegex: Option[Regex] = statsFilter.get flatMap { regexesString =>
    mkRegex(regexesString)
  }

  def apply(request: HttpRequest): Future[HttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    response.headers.add(HttpHeaders.Names.CONTENT_TYPE, MediaType.Json)

    val pretty = readBooleanParam(request, name = "pretty", default = false)
    val filtered = readBooleanParam(request, name = "filtered", default = false)

    response.setContent(ChannelBuffers.wrappedBuffer(json(pretty, filtered).getBytes))
    Future.value(response)
  }

  private[this] def readBooleanParam(request: HttpRequest, name: String, default: Boolean): Boolean = {
    try {
      val params = new QueryStringDecoder(request.getUri).getParameters
      Option(params.get(name)) match {
        case Some(p) => p.contains("1") || p.contains("true")
        case _ => default
      }
    } catch {
      case _: IllegalArgumentException => default
    }
  }

  def json(pretty: Boolean, filtered: Boolean): String = {
    import scala.collection.JavaConverters.mapAsScalaMapConverter

    val sample = registry.sample().asScala

    val sampleFiltered = if (filtered) filterSample(sample) else sample

    if (pretty) {
      // Create a TreeMap for sorting the keys
      val samples = immutable.TreeMap.empty[String, Number] ++ sampleFiltered
      prettyWriter.writeValueAsString(samples)
    } else {
      writer.writeValueAsString(sampleFiltered)
    }
  }

  def mkRegex(regexesString: String): Option[Regex] = {
    regexesString.split(",") match {
      case Array("") => None
      case regexes => Some(regexes.mkString("(", ")|(", ")").r)
    }
  }

  def filterSample(sample: collection.Map[String, Number]): collection.Map[String, Number] = {
    statsFilterRegex map { regex =>
      sample.filterKeys(! regex.pattern.matcher(_).matches)
    } getOrElse sample // no filtering was specified, return the original sample
  }
}