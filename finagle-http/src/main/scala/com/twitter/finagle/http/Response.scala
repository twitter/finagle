package com.twitter.finagle.http

import com.google.common.base.Charsets
import com.twitter.collection.RecordSchema
import com.twitter.finagle.http.netty.{HttpResponseProxy, Bijections}
import com.twitter.io.Reader
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.http.{
  DefaultHttpResponse, HttpResponse, HttpResponseDecoder, HttpResponseEncoder,
  HttpResponseStatus
}

import Bijections._

/**
 * Rich HttpResponse
 */
abstract class Response extends Message with HttpResponseProxy {

  /**
   * Arbitrary user-defined context associated with this response object.
   * [[com.twitter.collection.RecordSchema.Record RecordSchema.Record]] is
   * used here, rather than [[com.twitter.finagle.Context Context]] or similar
   * out-of-band mechanisms, to make the connection between the response and its
   * associated context explicit.
   */
  val ctx: Response.Schema.Record = Response.Schema.newRecord()

  def isRequest = false

  def status: Status          = from(getStatus)
  def status_=(value: Status) { setStatus(from(value)) }
  def statusCode: Int                     = getStatus.getCode
  def statusCode_=(value: Int)            { setStatus(HttpResponseStatus.valueOf(value)) }

  def getStatusCode(): Int      = statusCode
  def setStatusCode(value: Int) { statusCode = value }

  /** Encode as an HTTP message */
  def encodeString(): String = {
    val encoder = new EncoderEmbedder[ChannelBuffer](new HttpResponseEncoder)
    encoder.offer(httpResponse)
    val buffer = encoder.poll()
    buffer.toString(Charsets.UTF_8)
  }

  override def toString =
    "Response(\"" + version + " " + status + "\")"
}

object Response {
  /**
   * Utility class to make it possible to mock/spy a Response.
   */
  class Ok extends Response {
    val httpResponse = apply.httpResponse
  }

  /**
   * [[com.twitter.collection.RecordSchema RecordSchema]] declaration, used
   * to generate [[com.twitter.collection.RecordSchema.Record Record]] instances
   * for Response.ctx.
   */
  val Schema: RecordSchema = new RecordSchema

  /** Decode a [[Response]] from a String */
  def decodeString(s: String): Response = {
    decodeBytes(s.getBytes(Charsets.UTF_8))
  }

  /** Decode a [[Response]] from a byte array */
  def decodeBytes(b: Array[Byte]): Response = {
    val decoder = new DecoderEmbedder(
      new HttpResponseDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue))
    decoder.offer(ChannelBuffers.wrappedBuffer(b))
    val res = decoder.poll().asInstanceOf[HttpResponse]
    assert(res ne null)
    Response(res)
  }
  
  /** Create Response. */
  def apply(): Response =
    apply(Version.Http11, Status.Ok)

  /** Create Response from version and status. */
  def apply(version: Version, status: Status): Response =
    apply(new DefaultHttpResponse(from(version), from(status)))

  /**
   * Create a Response from version, status, and Reader.
   */
  def apply(version: Version, status: Status, reader: Reader): Response = {
    val res = new DefaultHttpResponse(from(version), from(status))
    res.setChunked(true)
    apply(res, reader)
  }

  private[http] def apply(response: HttpResponse): Response =
    new Response {
      val httpResponse = response
    }

  private[http] def apply(response: HttpResponse, readerIn: Reader): Response =
    new Response {
      val httpResponse = response
      override val reader = readerIn
    }

  /** Create Response from status. */
  def apply(status: Status): Response =
    apply(Version.Http11, status)

  /** Create Response from Request. */
  private[http] def apply(httpRequest: Request): Response =
    new Response {
      final val httpResponse =
        new DefaultHttpResponse(httpRequest.getProtocolVersion, HttpResponseStatus.OK)
    }
}
