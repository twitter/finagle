package com.twitter.finagle.http

import com.twitter.finagle.http.collection.RecordSchema
import com.twitter.finagle.http.util.FailingWriter
import com.twitter.io.Buf
import com.twitter.io.Pipe
import com.twitter.io.Reader
import com.twitter.io.Writer

/**
 * Rich HttpResponse
 */
abstract class Response private extends Message {

  /**
   * Arbitrary user-defined context associated with this response object.
   * [[com.twitter.finagle.http.collection.RecordSchema.Record RecordSchema.Record]] is
   * used here, rather than [[com.twitter.finagle.context.Context]] or similar
   * out-of-band mechanisms, to make the connection between the response and its
   * associated context explicit.
   */
  def ctx: Response.Schema.Record

  final def isRequest = false

  def status: Status

  /** Get Location header */
  def location: Option[String] = headerMap.get(Fields.Location)

  /** Set Location header */
  def location_=(value: String): Unit = headerMap.set(Fields.Location, value)

  /** Get Retry-After header */
  def retryAfter: Option[String] = headerMap.get(Fields.RetryAfter)

  /** Set Retry-After header */
  def retryAfter_=(value: String): Unit = headerMap.set(Fields.RetryAfter, value)

  /** Set Retry-After header by seconds */
  def retryAfter_=(value: Long): Unit = {
    retryAfter = value.toString
  }

  /** Get Server header */
  def server: Option[String] = headerMap.get(Fields.Server)

  /** Set Server header */
  def server_=(value: String): Unit = headerMap.set(Fields.Server, value)

  /** Get WWW-Authenticate header */
  def wwwAuthenticate: Option[String] = headerMap.get(Fields.WwwAuthenticate)

  /** Set WWW-Authenticate header */
  def wwwAuthenticate_=(value: String): Unit = headerMap.set(Fields.WwwAuthenticate, value)

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

  override def toString =
    "Response(\"" + version + " " + status + "\")"
}

object Response {

  /**
   * [[com.twitter.finagle.http.collection.RecordSchema RecordSchema]] declaration, used
   * to generate [[com.twitter.finagle.http.collection.RecordSchema.Record Record]] instances
   * for Response.ctx.
   */
  val Schema: RecordSchema = new RecordSchema

  /** Create Response. */
  def apply(): Response =
    apply(Version.Http11, Status.Ok)

  /** Create Response from status. */
  def apply(status: Status): Response =
    apply(Version.Http11, status)

  /** Create Response from version and status. */
  def apply(version: Version, status: Status): Response = {
    // Since this is a user made `Response` we use a Pipe so they
    // can keep a handle to the writer half and the server implementation can use
    // the reader half.
    val rw = new Pipe[Chunk]
    val resp = new Impl(rw, rw)
    resp.version = version
    resp.status = status
    resp
  }

  /**
   * Create a Response from version, status, and Reader.
   */
  def apply(version: Version, status: Status, reader: Reader[Buf]): Response = {
    chunked(version, status, reader)
  }

  private[finagle] def chunked(version: Version, status: Status, reader: Reader[Buf]): Response = {
    val resp = new Impl(reader.map(Chunk.apply))
    resp.version = version
    resp.status = status
    resp.setChunked(true)
    resp
  }

  /** Create 200 Response with the same HTTP version as the provided Request */
  def apply(request: Request): Response = apply(request.version, Status.Ok)

  /**
   * An inbound response (a response received by a client) that can include trailers.
   */
  private[finagle] final class Inbound(val chunkReader: Reader[Chunk], val trailers: HeaderMap)
      extends Response {

    @volatile private[this] var _status: Status = Status.Ok

    def chunkWriter: Writer[Chunk] = FailingWriter

    val headerMap: HeaderMap = HeaderMap()

    // Lazily created which allows those not using this functionality to not pay for it.
    @volatile private[this] var _ctx: Response.Schema.Record = _
    def ctx: Response.Schema.Record = {
      if (_ctx == null) synchronized {
        if (_ctx == null) _ctx = Response.Schema.newRecord()
      }
      _ctx
    }

    override def status: Status = _status
    override def status_=(value: Status): Unit = {
      _status = value
    }
  }

  /**
   * An outbound response (a response sent by a server).
   */
  private[finagle] final class Impl(val chunkReader: Reader[Chunk], val chunkWriter: Writer[Chunk])
      extends Response {

    def this(chunkReader: Reader[Chunk]) = this(chunkReader, FailingWriter)

    @volatile private[this] var _status: Status = Status.Ok

    val headerMap: HeaderMap = HeaderMap()

    // Lazily created which allows those not using this functionality to not pay for it.
    @volatile private[this] var _ctx: Response.Schema.Record = _
    def ctx: Response.Schema.Record = {
      if (_ctx == null) synchronized {
        if (_ctx == null) _ctx = Response.Schema.newRecord()
      }
      _ctx
    }

    def trailers: HeaderMap = HeaderMap.Empty

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

    def chunkReader: Reader[Chunk] = response.chunkReader
    def chunkWriter: Writer[Chunk] = response.chunkWriter
    def ctx: Response.Schema.Record = response.ctx
    override lazy val cookies: CookieMap = response.cookies
    def headerMap: HeaderMap = response.headerMap
    def trailers: HeaderMap = response.trailers

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
