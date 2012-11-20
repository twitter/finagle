package com.twitter.finagle.http

import com.twitter.finagle.http.netty.HttpResponseProxy
import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpRequest, HttpResponse,
  HttpResponseStatus, HttpVersion}


/**
 * Rich HttpResponse
 */
abstract class Response extends Message with HttpResponseProxy {

  def isRequest = false

  def status: HttpResponseStatus          = getStatus
  def status_=(value: HttpResponseStatus) { setStatus(value) }
  def statusCode: Int                     = getStatus.getCode
  def statusCode_=(value: Int)            { setStatus(HttpResponseStatus.valueOf(value)) }

  def getStatusCode(): Int      = statusCode
  def setStatusCode(value: Int) { statusCode = value }

  override def toString =
    "Response(\"" + version + " " + status + "\")"
}


class MockResponse extends Response {
  val httpResponse = new DefaultHttpResponse(Version.Http11, Status.Ok)
}


object Response {

  /** Create Response.  Convenience method for testing. */
  def apply(): Response =
    apply(Version.Http11, Status.Ok)

  /** Create Response from version and status.  Convenience method for testing. */
  def apply(version: HttpVersion, status: HttpResponseStatus): Response =
    apply(new DefaultHttpResponse(version, status))

  /** Create Response from HttpResponse. */
  def apply(httpResponseArg: HttpResponse): Response =
    new Response {
      final val httpResponse = httpResponseArg
    }

  /** Create Response from HttpRequest. */
  def apply(httpRequest: HttpRequest): Response =
    new Response {
      final val httpResponse =
        new DefaultHttpResponse(httpRequest.getProtocolVersion, Status.Ok)
  }
}
