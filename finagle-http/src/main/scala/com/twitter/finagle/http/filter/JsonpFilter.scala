package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{MediaType, Method, Request, Response}
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers


/**
 * JSONP (callback) filter
 *
 * Wrap JSON content in <callback>(<content>);
 *
 * See: http://en.wikipedia.org/wiki/JSONP
 */
class JsonpFilter[REQUEST <: Request] extends SimpleFilter[REQUEST, Response] {

  def apply(request: REQUEST, service: Service[REQUEST, Response]): Future[Response] = {
    getCallback(request) match {
      case Some(callback) =>
        service(request) onSuccess { response =>
          if (response.mediaType == Some(MediaType.Json)) {
            response.content =
              ChannelBuffers.wrappedBuffer(
                ChannelBuffers.wrappedBuffer(callback.getBytes("UTF-8")),
                ChannelBuffers.wrappedBuffer(JsonpFilter.LeftParen),
                response.getContent,
                ChannelBuffers.wrappedBuffer(JsonpFilter.RightParenSemicolon))
            response.mediaType = MediaType.Javascript
          }
        }
      case None =>
        service(request)
    }
  }

  def getCallback(request: Request): Option[String] = {
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


object JsonpFilter extends JsonpFilter[Request] {
  // Sanitize to prevent cross domain policy attacks and such
  private val SanitizerRegex = """[^\/\@\.\[\]\:\w\d]""".r

  // Reuse left/right paren.  The semicolon may not be strictly necessary, but
  // some APIs include it.
  private val LeftParen  = Array('('.toByte)
  private val RightParenSemicolon = ");".getBytes
}
