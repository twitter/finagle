package com.twitter.finagle.http

import com.google.common.base.Charsets
import com.twitter.finagle.http.netty.HttpResponseProxy
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.Channel
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.http._

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

  /** Encode as an HTTP message */
  def encodeString(): String = {
    val encoder = new EncoderEmbedder[ChannelBuffer](new HttpResponseEncoder)
    encoder.offer(this)
    val buffer = encoder.poll()
    buffer.toString(Charsets.UTF_8)
  }

  override def toString =
    "Response(\"" + version + " " + status + "\")"
}


class MockResponse extends Response {
  val httpResponse = new DefaultHttpResponse(Version.Http11, Status.Ok)
}


object Response {

  /** Decode a Response from a String */
  def decodeString(s: String): Response = {
    val decoder = new DecoderEmbedder(
      new HttpResponseDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue))
    decoder.offer(ChannelBuffers.wrappedBuffer(s.getBytes(Charsets.UTF_8)))
    val httpResponse = decoder.poll().asInstanceOf[HttpResponse]
    assert(httpResponse ne null)
    Response(httpResponse)
  }

  /** Create Response. */
  def apply(): Response =
    apply(Version.Http11, Status.Ok)

  /** Create Response from version and status. */
  def apply(version: HttpVersion, status: HttpResponseStatus): Response =
    apply(new DefaultHttpResponse(version, status))

  /** Create Response from status. */
  def apply(status: HttpResponseStatus): Response =
    apply(new DefaultHttpResponse(Version.Http11, status))

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
