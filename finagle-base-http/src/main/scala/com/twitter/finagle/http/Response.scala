package com.twitter.finagle.http

import com.google.common.base.Charsets
import com.twitter.collection.RecordSchema
import com.twitter.finagle.http.netty.Bijections
import com.twitter.io.Reader
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.http._

import Bijections._

/**
 * Rich HttpResponse
 */
abstract class Response extends Message {

  /**
   * Arbitrary user-defined context associated with this response object.
   * [[com.twitter.collection.RecordSchema.Record RecordSchema.Record]] is
   * used here, rather than [[com.twitter.finagle.Context Context]] or similar
   * out-of-band mechanisms, to make the connection between the response and its
   * associated context explicit.
   */
  val ctx: Response.Schema.Record = Response.Schema.newRecord()

  def isRequest = false

  def status: Status = Bijections.statusFromNetty(httpResponse.getStatus)

  /**
   * Set the status of this response
   *
   * @note see [[status(Status)]] for Java users.
   */
  def status_=(value: Status): Unit = { httpResponse.setStatus(from(value)) }

  /**
   * Set the status of this response
   *
   * @note See [[status_=(Status)]] for Scala users.
   */
  final def status(value: Status): this.type = {
    this.status = value
    this
  }

  /**
   * Get the status code of this response
   */
  def statusCode: Int = status.code

  /**
   * Set the status code of this response
   *
   * @note See [[statusCode(Int)]] for Java users.
   */
  def statusCode_=(value: Int): Unit = { httpResponse.setStatus(HttpResponseStatus.valueOf(value)) }

  /**
   * Set the status code of this response
   *
   * @note See [[statusCode_=(Int)]] for Scala users.
   */
  final def statusCode(value: Int): this.type = {
    this.statusCode = value
    this
  }

  /** Get the status code of this response */
  @deprecated("2017-02-17", "Use [[statusCode]] instead")
  final def getStatusCode(): Int = statusCode

  /** Set the status code of this response */
  @deprecated("2017-02-17", "Use [[statusCode(Int)]] instead")
  final def setStatusCode(value: Int): Unit = { statusCode = value }

  /** Encode as an HTTP message */
  def encodeString(): String = {
    val encoder = new EncoderEmbedder[ChannelBuffer](new HttpResponseEncoder)
    encoder.offer(httpResponse)
    val buffer = encoder.poll()
    buffer.toString(Charsets.UTF_8)
  }

  override def toString =
    "Response(\"" + version + " " + status + "\")"

  @deprecated("Going away as part of the Netty 4 transition", "2017-01-26")
  protected def httpResponse: HttpResponse

  @deprecated("Going away as part of the Netty 4 transition", "2017-01-26")
  protected[finagle] def httpMessage: HttpMessage = httpResponse
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

  /** Create Response from status. */
  def apply(status: Status): Response =
    apply(Version.Http11, status)

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

  /** Create 200 Response with the same HTTP version as the provided Request */
  def apply(request: Request): Response =
    new Response {
      final val httpResponse =
        new DefaultHttpResponse(from(request.version), HttpResponseStatus.OK)
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
}
