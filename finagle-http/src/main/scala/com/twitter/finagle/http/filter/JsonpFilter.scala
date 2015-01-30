package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{MediaType, Method, Ask, Response}
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers


/**
 * JSONP (callback) filter
 *
 * Wrap JSON content in <callback>(<content>);
 *
 * See: http://en.wikipedia.org/wiki/JSONP
 */
class JsonpFilter[ASK <: Ask] extends SimpleFilter[ASK, Response] {

  def apply(request: ASK, service: Service[ASK, Response]): Future[Response] = {
    getCallback(request) match {
      case Some(callback) =>
        addCallback(callback, request, service)
      case None =>
        service(request)
    }
  }

  def addCallback(callback: String, request: ASK, service: Service[ASK, Response]): Future[Response] =
    service(request) map { response =>
      if (response.mediaType == Some(MediaType.Json)) {
        response.content =
          ChannelBuffers.wrappedBuffer(
            ChannelBuffers.wrappedBuffer(JsonpFilter.Comment),
            ChannelBuffers.wrappedBuffer(callback.getBytes("UTF-8")),
            ChannelBuffers.wrappedBuffer(JsonpFilter.LeftParen),
            response.getContent,
            ChannelBuffers.wrappedBuffer(JsonpFilter.RightParenSemicolon))
        response.mediaType = MediaType.Javascript
      }
      response
    }


  def getCallback(request: Ask): Option[String] = {
    // Ignore HEAD, though in practice this should be behind the HeadFilter
    if (request.method != Method.Head)
      request.params.get("callback") flatMap { callback =>
        val sanitizedCallback = JsonpFilter.SanitizerRegex.replaceAllIn(callback, "")
        if (!sanitizedCallback.isEmpty)
          Some(sanitizedCallback)
        else
          None
      }
    else
      None
  }
}


object JsonpFilter extends JsonpFilter[Ask] {
  // Sanitize to prevent cross domain policy attacks and such
  private val SanitizerRegex = """[^\/\@\.\[\]\:\w\d]""".r

  // Reuse left/right paren.  The semicolon may not be strictly necessary, but
  // some APIs include it.
  private val LeftParen  = Array('('.toByte)
  private val RightParenSemicolon = ");".getBytes
  // Prepended to address CVE-2014-4671
  private val Comment = "/**/".getBytes
}
