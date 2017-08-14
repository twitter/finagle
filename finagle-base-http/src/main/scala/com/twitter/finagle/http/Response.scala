package com.twitter.finagle.http

import com.google.common.base.Charsets
import com.twitter.collection.RecordSchema
import com.twitter.finagle.http.netty.Bijections
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util.Closable
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.http._
import Bijections._

/**
 * Rich HttpResponse
 */
abstract class Response private extends Message {

  /**
   * Arbitrary user-defined context associated with this response object.
   * [[com.twitter.collection.RecordSchema.Record RecordSchema.Record]] is
   * used here, rather than [[com.twitter.finagle.context.Context]] or similar
   * out-of-band mechanisms, to make the connection between the response and its
   * associated context explicit.
   */
  val ctx: Response.Schema.Record = Response.Schema.newRecord()

  final def isRequest = false

  def status: Status

  /**
   * Set the status of this response
   *
   * @note see [[status(Status)]] for Java users.
   */
  def status_=(value: Status): Unit

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
  final def statusCode: Int = status.code

  /**
   * Set the status code of this response
   *
   * @note See [[statusCode(Int)]] for Java users.
   */
  final def statusCode_=(value: Int): Unit = status = Status.fromCode(value)

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
    encoder.offer(Bijections.responseToNetty(this))
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
  @deprecated("Use Response or Response.Proxy", "2017-04-28")
  class Ok extends Proxy {
    val response = Response()
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
      new HttpResponseDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue)
    )
    decoder.offer(ChannelBuffers.wrappedBuffer(b))
    val res = decoder.poll().asInstanceOf[HttpResponse]
    assert(res ne null)
    responseFromNetty(res)
  }

  /** Create Response. */
  def apply(): Response =
    apply(Version.Http11, Status.Ok)

  /** Create Response from status. */
  def apply(status: Status): Response =
    apply(Version.Http11, status)

  /** Create Response from version and status. */
  def apply(version: Version, status: Status): Response = {
    // Since this is a user made `Response` we use the joined Reader.writable so they
    // can keep a handle to the writer half and the server implementation can use
    // the reader half.
    val rw = Reader.writable()
    val resp = new ResponseImpl(rw, rw)
    resp.version = version
    resp.status = status
    resp
  }

  /**
   * Create a Response from version, status, and Reader.
   */
  def apply(version: Version, status: Status, reader: Reader): Response = {
    chunked(version, status, reader)
  }

  private[finagle] def chunked(version: Version, status: Status, reader: Reader): Response = {
    val resp = new ResponseImpl(reader, Writer.FailingWriter)
    resp.version = version
    resp.status = status
    resp.setChunked(true)
    resp
  }

  /** Create 200 Response with the same HTTP version as the provided Request */
  def apply(request: Request): Response = apply(request.version, Status.Ok)

  final private class ResponseImpl(val reader: Reader, val writer: Writer with Closable)
      extends Response {
    private[this] var _status: Status = Status.Ok
    override def status: Status = _status
    override def status_=(value: Status): Unit = {
      _status = value
    }
  }

  abstract class Proxy extends Response {

    /**
     * Underlying `Response`
     */
    def response: Response

    def reader: Reader = response.reader
    def writer: Writer with Closable = response.writer
    override def headerMap: HeaderMap = response.headerMap

    // These things should never need to be modified
    final def status: Status = response.status
    final def status_=(value: Status): Unit = response.status_=(value)
    final override def content: Buf = response.content
    final override def content_=(content: Buf): Unit = response.content_=(content)
    final override def version: Version = response.version
    final override def version_=(version: Version): Unit = response.version_=(version)
    final override def isChunked: Boolean = response.isChunked
    final override def setChunked(chunked: Boolean): Unit = response.setChunked(chunked)

  }

}
