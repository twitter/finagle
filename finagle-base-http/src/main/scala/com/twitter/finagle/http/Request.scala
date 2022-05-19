package com.twitter.finagle.http

import com.twitter.finagle.http.collection.RecordSchema
import com.twitter.finagle.http.util.FailingWriter
import com.twitter.io.Buf
import com.twitter.io.Pipe
import com.twitter.io.Reader
import com.twitter.io.Writer
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.AbstractMap
import java.util.{List => JList}
import java.util.{Map => JMap}
import java.util.{Set => JSet}
import scala.beans.BeanProperty
import scala.annotation.varargs
import scala.collection.JavaConverters._

/**
 * Rich HttpRequest.
 *
 * Use RequestProxy to create an even richer subclass.
 */
abstract class Request private extends Message {

  /**
   * Arbitrary user-defined context associated with this request object.
   * [[com.twitter.finagle.http.collection.RecordSchema.Record RecordSchema.Record]] is
   * used here, rather than [[com.twitter.finagle.context.Context Context]] or similar
   * out-of-band mechanisms, to make the connection between the request and its
   * associated context explicit.
   */
  def ctx: Request.Schema.Record

  final def isRequest: Boolean = true

  /**
   * Returns a [[ParamMap]] instance, which contains both parameters provided
   * as part of the request URI and parameters provided as part of the request
   * body.
   *
   * @note Request body parameters are considered if the following criteria are true:
   *   1. The request is not a TRACE request.
   *   2. The request media type is 'application/x-www-form-urlencoded'
   *   3. The content length is greater than 0.
   *
   * {{{
   * import com.twitter.finagle.http.{MediaType, Method, Request}
   *
   * val request = Request(Method.Post, "/search?a=yes")
   * request.mediaType = MediaType.WwwForm
   * request.contentString = "a=no&b=yes&c=no"
   * request.params
   *
   * // Result
   * // com.twitter.finagle.http.ParamMap = ?c=no&a=no&b=yes&a=yes
   * }}}
   *
   * To get just query parameters from the URI, use [[Uri]].
   */
  def params: ParamMap = _params
  private[this] lazy val _params: ParamMap = new RequestParamMap(this)

  /**
   * Returns the HTTP method of this request.
   */
  def method: Method

  /**
   * Sets the HTTP method of this request to the given `method`.
   *
   * * @see [[method(Method)]] for Java users.
   */
  def method_=(method: Method): Unit

  /**
   * Sets the HTTP method of this request to the given `method`.
   *
   * @see [[method_=(Method)]] for Scala users.
   */
  final def method(method: Method): this.type = {
    this.method = method
    this
  }

  /**
   * Returns the URI of this request.
   */
  def uri: String

  /**
   * Set the URI of this request.
   *
   * @see [[uri_=(String)]] for Scala users.
   */
  final def uri(value: String): this.type = {
    uri = value
    this
  }

  /**
   * Set the URI of this request to the given `uri`.
   *
   * @see [[uri(String)]] for Java users.
   */
  def uri_=(uri: String): Unit

  /** Accept header */
  def accept: Seq[String] =
    headerMap.get(Fields.Accept) match {
      case Some(s) => s.split(",").map(_.trim).filter(_.nonEmpty)
      case None => Seq()
    }

  /** Set Accept header */
  def accept_=(value: String): Unit = headerMap.set(Fields.Accept, value)

  /** Set Accept header with list of values */
  def accept_=(values: Iterable[String]): Unit = accept = values.mkString(", ")

  /** Accept header media types (normalized, no parameters) */
  def acceptMediaTypes: Seq[String] =
    accept.flatMap {
      _.split(";", 2).headOption
        .map(_.trim.toLowerCase) // media types are case-insensitive
        .filter(_.nonEmpty) // skip blanks
    }

  /** Get Authorization header */
  def authorization: Option[String] = headerMap.get(Fields.Authorization)

  /** Set Authorization header */
  def authorization_=(value: String): Unit = headerMap.set(Fields.Authorization, value)

  /** Get Host header */
  def host: Option[String] = headerMap.get(Fields.Host)

  /**
   * Set Host header
   *
   * @see host(String) for Java users
   */
  def host_=(value: String): Unit = headerMap.set(Fields.Host, value)

  /**
   * Set the Host header
   *
   * @see [[host_=(String)]] for Scala users
   */
  final def host(value: String): this.type = {
    host = value
    this
  }

  /** Get Referer [sic] header */
  def referer: Option[String] = headerMap.get(Fields.Referer)

  /** Set Referer [sic] header */
  def referer_=(value: String): Unit = headerMap.set(Fields.Referer, value)

  /** Get User-Agent header */
  def userAgent: Option[String] = headerMap.get(Fields.UserAgent)

  /** Set User-Agent header */
  def userAgent_=(value: String): Unit = headerMap.set(Fields.UserAgent, value)

  /** Get X-Forwarded-For header */
  def xForwardedFor: Option[String] = headerMap.get("X-Forwarded-For")

  /** Set X-Forwarded-For header */
  def xForwardedFor_=(value: String): Unit = headerMap.set("X-Forwarded-For", value)

  /** Path from URI. */
  @BeanProperty
  def path: String = {
    val u = uri
    u.indexOf('?') match {
      case -1 => u
      case n => u.substring(0, n)
    }
  }

  /** File extension.  Empty string if none. */
  @BeanProperty
  def fileExtension: String = {
    val p = path
    val leaf = p.lastIndexOf('/') match {
      case -1 => p
      case n => p.substring(n + 1)
    }
    leaf.lastIndexOf('.') match {
      case -1 => ""
      case n => leaf.substring(n + 1).toLowerCase
    }
  }

  /**
   * The InetSocketAddress of the client or a place-holder
   * ephemeral address for requests that have yet to be dispatched.
   */
  @BeanProperty
  def remoteSocketAddress: InetSocketAddress

  /** Remote host - a dotted quad */
  @BeanProperty
  def remoteHost: String =
    remoteAddress.getHostAddress

  /** Remote InetAddress */
  @BeanProperty
  def remoteAddress: InetAddress =
    remoteSocketAddress.getAddress

  /** Remote port */
  @BeanProperty
  def remotePort: Int =
    remoteSocketAddress.getPort

  // The get*Param methods below are for Java compatibility.  Note Scala default
  // arguments aren't compatible with Java, so we need two versions of each.

  /**
   * Get parameter value. Returns value or null.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getParam(name: String): String =
    getParam(name, null)

  /**
   * Get parameter value. Returns value or default.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getParam(name: String, default: String): String =
    params.getOrElse(name, default)

  /**
   * Get Short param. Returns value or 0.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getShortParam(name: String): Short =
    params.getShortOrElse(name, 0)

  /**
   * Get Short param. Returns value or default.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getShortParam(name: String, default: Short): Short =
    params.getShortOrElse(name, default)

  /**
   * Get Int param. Returns value or 0.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getIntParam(name: String): Int =
    params.getIntOrElse(name, 0)

  /**
   * Get Int param. Returns value or default.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getIntParam(name: String, default: Int): Int =
    params.getIntOrElse(name, default)

  /**
   * Get Long param. Returns value or 0.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getLongParam(name: String): Long =
    params.getLongOrElse(name, 0L)

  /**
   * Get Long param. Returns value or default.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getLongParam(name: String, default: Long): Long =
    params.getLongOrElse(name, default)

  /**
   * Get Boolean param. Returns value or false.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getBooleanParam(name: String): Boolean =
    params.getBooleanOrElse(name, false)

  /**
   * Get Boolean param. Returns value or default.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getBooleanParam(name: String, default: Boolean): Boolean =
    params.getBooleanOrElse(name, default)

  /**
   * Get all values of parameter. Returns list of values.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getParams(name: String): JList[String] =
    params.getAll(name).toList.asJava

  /**
   * Get all parameters.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getParams(): JList[JMap.Entry[String, String]] =
    params.toList.map {
      case (k, v) =>
        // cast to appease asJava
        new AbstractMap.SimpleImmutableEntry(k, v).asInstanceOf[JMap.Entry[String, String]]
    }.asJava

  /**
   * Check if parameter exists.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def containsParam(name: String): Boolean =
    params.contains(name)

  /**
   * Get parameters names.
   *
   * @see [[params]] for details of which parameters are considered.
   */
  def getParamNames(): JSet[String] =
    params.keySet.asJava

  /** Response associated with request. */
  @deprecated("Use the Response constructor functions", "2016-12-29")
  lazy val response: Response = Response(this)

  /** Get response associated with request. */
  @deprecated("Use the Response constructor functions", "2016-12-29")
  def getResponse(): Response = response

  override def toString: String =
    s"""Request("$method $uri", from $remoteSocketAddress)"""
}

object Request {

  /**
   * [[com.twitter.finagle.http.collection.RecordSchema RecordSchema]] declaration, used
   * to generate [[com.twitter.finagle.http.collection.RecordSchema.Record Record]] instances
   * for Request.ctx.
   */
  val Schema: RecordSchema = new RecordSchema

  /**
   * Create an HTTP/1.1 GET Request from query string parameters.
   *
   * @param params a list of key-value pairs representing the query string.
   */
  @varargs
  def apply(params: Tuple2[String, String]*): Request =
    apply("/", params: _*)

  /**
   * Create an HTTP/1.1 GET Request from URI and query string parameters.
   *
   * @param params a list of key-value pairs representing the query string.
   */
  def apply(uri: String, params: Tuple2[String, String]*): Request =
    apply(Method.Get, queryString(uri, params: _*))

  /**
   * Create an HTTP/1.1 GET request from URI string.
   */
  def apply(uri: String): Request =
    apply(Method.Get, uri)

  /**
   * Create an HTTP/1.1 request from method and URI string.
   */
  def apply(method: Method, uri: String): Request =
    apply(Version.Http11, method, uri)

  /**
   * Create an HTTP/1.1 request from version, method, and URI string.
   */
  def apply(version: Version, method: Method, uri: String): Request = {
    // Since this is a user made `Request` we use a Pipe so they
    // can keep a handle to the writer half and the client implementation can use
    // the reader half.
    val rw = new Pipe[Chunk]
    val req = new Request.Impl(rw, rw, new InetSocketAddress(0))

    req.version = version
    req.method = method
    req.uri = uri
    req
  }

  /**
   * Create an HTTP/1.1 request from Version, Method, URI, and Reader.
   *
   * A [[com.twitter.io.Reader]] is a stream of bytes serialized to HTTP chunks.
   * `Reader`s are useful for representing streaming data in the body of the
   * request (e.g. a large file, or long lived computation that produces results
   * incrementally).
   *
   * {{{
   * val data = Reader.fromStream(File.open("data.txt"))
   * val post = Request(Http11, Post, "/upload", data)
   *
   * client(post) onSuccess {
   *   case r if r.status == Ok => println("Success!")
   *   case _                   => println("Something went wrong...")
   * }
   * }}}
   */
  def apply(version: Version, method: Method, uri: String, reader: Reader[Buf]): Request = {
    val req = new Request.Impl(
      reader.map(Chunk.apply),
      new InetSocketAddress(0)
    )

    req.setChunked(true)
    req.version = version
    req.method = method
    req.uri = uri
    req
  }

  /** Create a query string from URI and parameters. */
  def queryString(uri: String, params: Tuple2[String, String]*): String = {
    uri + QueryParamEncoder.encode(params)
  }

  /**
   * Create a query string from parameters.  The results begins with "?" only if
   * params is non-empty.
   */
  def queryString(params: Tuple2[String, String]*): String =
    queryString("", params: _*)

  /** Create a query string from URI and parameters. */
  def queryString(uri: String, params: Map[String, String]): String =
    queryString(uri, params.toSeq: _*)

  /**
   * Create a query string from parameters.  The results begins with "?" only if
   * params is non-empty.
   */
  def queryString(params: Map[String, String]): String =
    queryString("", params.toSeq: _*)

  /**
   * Proxy for Request.  This can be used to create a richer request class
   * that wraps Request without exposing the underlying netty http type.
   */
  abstract class Proxy extends Request {

    /**
     * Underlying `Request`
     */
    def request: Request

    def ctx: Schema.Record = request.ctx
    def remoteSocketAddress: InetSocketAddress = request.remoteSocketAddress
    def chunkReader: Reader[Chunk] = request.chunkReader
    def chunkWriter: Writer[Chunk] = request.chunkWriter
    override lazy val cookies: CookieMap = request.cookies
    def headerMap: HeaderMap = request.headerMap
    def trailers: HeaderMap = request.trailers
    override def params: ParamMap = request.params
    override lazy val response: Response = request.response
    def uri: String = request.uri

    // These should never need to be overridden
    final def method: Method = request.method
    final def method_=(method: Method): Unit = request.method_=(method)
    final def uri_=(uri: String): Unit = request.uri_=(uri)
    final override def content: Buf = request.content
    final override def content_=(content: Buf): Unit = request.content_=(content)
    final override def version: Version = request.version
    final override def version_=(version: Version): Unit = request.version_=(version)
    final override def isChunked: Boolean = request.isChunked
    final override def setChunked(chunked: Boolean): Unit = request.setChunked(chunked)
  }

  /**
   * An inbound request (a request received by a server) that could include trailers.
   */
  private[finagle] final class Inbound(
    val chunkReader: Reader[Chunk],
    val remoteSocketAddress: InetSocketAddress,
    val trailers: HeaderMap)
      extends Request {

    def chunkWriter: Writer[Chunk] = FailingWriter

    @volatile private var _method: Method = Method.Get
    @volatile private var _uri: String = ""

    val headerMap: HeaderMap = HeaderMap()

    // Lazily created which allows those not using this functionality to not pay for it.
    @volatile private[this] var _ctx: Request.Schema.Record = _
    def ctx: Request.Schema.Record = {
      if (_ctx == null) synchronized {
        if (_ctx == null) _ctx = Request.Schema.newRecord()
      }
      _ctx
    }

    def method: Method = _method
    def method_=(method: Method): Unit = {
      _method = method
    }

    def uri: String = _uri
    def uri_=(uri: String): Unit = {
      _uri = uri
    }
  }

  /**
   * An outbound request (a request sent by a client).
   */
  private[finagle] final class Impl(
    val chunkReader: Reader[Chunk],
    val chunkWriter: Writer[Chunk],
    val remoteSocketAddress: InetSocketAddress)
      extends Request {

    def this(chunkReader: Reader[Chunk], remoteSocketAddress: InetSocketAddress) =
      this(chunkReader, FailingWriter, remoteSocketAddress)

    @volatile private var _method: Method = Method.Get
    @volatile private var _uri: String = ""

    val headerMap: HeaderMap = HeaderMap()

    // Lazily created which allows those not using this functionality to not pay for it.
    @volatile private[this] var _ctx: Request.Schema.Record = _
    def ctx: Request.Schema.Record = {
      if (_ctx == null) synchronized {
        if (_ctx == null) _ctx = Request.Schema.newRecord()
      }
      _ctx
    }

    def trailers: HeaderMap = HeaderMap.Empty

    def method: Method = _method
    def method_=(method: Method): Unit = {
      _method = method
    }

    def uri: String = _uri
    def uri_=(uri: String): Unit = {
      _uri = uri
    }
  }
}
