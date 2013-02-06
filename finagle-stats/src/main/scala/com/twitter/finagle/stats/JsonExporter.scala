package com.twitter.finagle.stats

import collection.immutable.TreeMap
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.common.metrics.Metrics
import com.twitter.finagle.Service
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

class JsonExporter(registry: Metrics) extends Service[HttpRequest, HttpResponse] {
  private[this] val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  private[this] val writer = mapper.writer

  private[this] val prettyWriter = {
    val printer = new DefaultPrettyPrinter
    printer.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter)
    mapper.writer(printer)
  }

  def apply(request: HttpRequest): Future[HttpResponse] = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val pretty = request.getHeader("pretty") match {
      case "1" | "true" => true
      case _ => false
    }
    response.setContent(ChannelBuffers.wrappedBuffer(json(pretty).getBytes))
    Future.value(response)
  }

  def json(pretty: Boolean): String = {
    import scala.collection.JavaConversions._

    // Create a TreeMap for sorting the keys
    val samples = TreeMap.empty[String, Number] ++ registry.sample()
    val printer = if (pretty) writer else prettyWriter
    printer.writeValueAsString(samples)
  }
}
