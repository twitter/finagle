package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{MediaType, Method, Request, Response}
import com.twitter.util.Future
import com.twitter.io.Buf

/**
 * JSONP (callback) filter
 *
 * Wrap JSON content in <callback>(<content>);
 *
 * See: https://en.wikipedia.org/wiki/JSONP
 */
class JsonpFilter[Req <: Request] extends SimpleFilter[Req, Response] {

  def apply(request: Req, service: Service[Req, Response]): Future[Response] = {
    getCallback(request) match {
      case Some(callback) =>
        addCallback(callback, request, service)
      case None =>
        service(request)
    }
  }

  def addCallback(
    callback: String,
    request: Req,
    service: Service[Req, Response]
  ): Future[Response] =
    service(request).map { response =>
      if (response.mediaType.contains(MediaType.Json)) {
        response.content = Buf(
          Seq(
            JsonpFilter.Comment,
            Buf.Utf8(callback),
            JsonpFilter.LeftParen,
            response.content,
            JsonpFilter.RightParenSemicolon
          )
        )
        response.mediaType = MediaType.Javascript
        response.contentLength match {
          case Some(len: Long) =>
            response.contentLength = len + callback.length + JsonpFilter.ExtraCharacters
          case None => // Likely using Transfer-Encoding instead, no-op
        }
      }
      response
    }

  def getCallback(request: Request): Option[String] = {
    // Ignore HEAD, though in practice this should be behind the HeadFilter
    if (request.method != Method.Head)
      request.params.get("callback").flatMap { callback =>
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
  private val LeftParen = Buf.Utf8("(")
  private val RightParenSemicolon = Buf.Utf8(");")
  // Prepended to address CVE-2014-4671
  private val Comment = Buf.Utf8("/**/")
  private val ExtraCharacters = 7
}
