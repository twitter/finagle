package com.twitter.finagle.http

import com.twitter.collection.RecordSchema
import com.twitter.finagle.http.exp.Multipart
import com.twitter.finagle.http.netty.{HttpRequestProxy, Bijections}
import com.twitter.io.{Charsets, Reader}
import java.net.{InetAddress, InetSocketAddress}
import java.util.{AbstractMap, List => JList, Map => JMap, Set => JSet}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.Channel
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.http._
import scala.beans.BeanProperty
import scala.annotation.varargs
import scala.collection.JavaConverters._

import Bijections._

/**
 * Rich HttpRequest.
 *
 * Use RequestProxy to create an even richer subclass.
 */
abstract class Request extends Message with HttpRequestProxy {

  /**
   * Arbitrary user-defined context associated with this request object.
   * [[com.twitter.collection.RecordSchema.Record RecordSchema.Record]] is
   * used here, rather than [[com.twitter.finagle.context.Context Context]] or similar
   * out-of-band mechanisms, to make the connection between the request and its
   * associated context explicit.
   */
  def ctx: Request.Schema.Record = _ctx
  private[this] val _ctx = Request.Schema.newRecord()

  def isRequest = true

  /**
   * Returns a [[ParamMap]] instance, which maintains query string and url-encoded
   * params associated with this request.
   */
  def params: ParamMap = _params
  private[this] lazy val _params: ParamMap = new RequestParamMap(this)

  /**
   * Returns an _optional_ [[Multipart]] instance, which maintains the
   * `multipart/form-data` content of this non-chunked, POST request. If
   * this requests is either streaming or non-POST, this method returns
   * `None`.
   *
   * Note: This method is a part of an experimental API for handling
   * multipart HTTP data and it will likely be changed in future in order
   * to support streaming requests.
   */
  def multipart: Option[Multipart] = _multipart
  private[this] lazy val _multipart: Option[Multipart] =
    if (!isChunked && method == Method.Post)
      Some(Multipart.decodeNonChunked(this))
    else None

  /**
   * Returns the HTTP method of this request.
   */
  def method: Method = from(getMethod())

  /**
   * Sets the HTTP method of this request to the given `method`.
   */
  def method_=(method: Method) = setMethod(from(method))

  /**
   * Returns the URI of this request.
   */
  def uri: String = getUri()

  /**
   * Set the URI of this request to the given `uri`.
   */
  def uri_=(uri: String) = setUri(uri)

  /** Path from URI. */
  @BeanProperty
  def path: String = {
    val u = uri
    u.indexOf('?') match {
      case -1 => u
      case n  => u.substring(0, n)
    }
  }

  /** File extension.  Empty string if none. */
  @BeanProperty
  def fileExtension: String = {
    val p = path
    val leaf = p.lastIndexOf('/') match {
      case -1 => p
      case n  => p.substring(n + 1)
    }
    leaf.lastIndexOf('.') match {
      case -1 => ""
      case n  => leaf.substring(n + 1).toLowerCase
    }
  }

  /** Remote InetSocketAddress */
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

  /** Get parameter value.  Returns value or null. */
  def getParam(name: String): String =
    params.get(name).orNull

  /** Get parameter value.  Returns value or default. */
  def getParam(name: String, default: String): String =
    params.get(name).getOrElse(default)

  /** Get Short param.  Returns value or 0. */
  def getShortParam(name: String): Short =
    params.getShortOrElse(name, 0)

  /** Get Short param.  Returns value or default. */
  def getShortParam(name: String, default: Short): Short =
    params.getShortOrElse(name, default)

  /** Get Int param.  Returns value or 0. */
  def getIntParam(name: String): Int =
    params.getIntOrElse(name, 0)

  /** Get Int param.  Returns value or default. */
  def getIntParam(name: String, default: Int): Int =
    params.getIntOrElse(name, default)

  /** Get Long param.  Returns value or 0. */
  def getLongParam(name: String): Long =
    params.getLongOrElse(name, 0L)

  /** Get Long param.  Returns value or default. */
  def getLongParam(name: String, default: Long=0L): Long =
    params.getLongOrElse(name, default)

  /** Get Boolean param.  Returns value or false. */
  def getBooleanParam(name: String): Boolean =
    params.getBooleanOrElse(name, false)

  /** Get Boolean param.  Returns value or default. */
  def getBooleanParam(name: String, default: Boolean): Boolean =
    params.getBooleanOrElse(name, default)

  /** Get all values of parameter.  Returns list of values. */
  def getParams(name: String): JList[String] =
    params.getAll(name).toList.asJava

  /** Get all parameters. */
  def getParams(): JList[JMap.Entry[String, String]] =
    (params.toList.map { case (k, v) =>
      // cast to appease asJava
      (new AbstractMap.SimpleImmutableEntry(k, v)).asInstanceOf[JMap.Entry[String, String]]
    }).asJava

  /** Check if parameter exists. */
  def containsParam(name: String): Boolean =
    params.contains(name)

  /** Get parameters names. */
  def getParamNames(): JSet[String] =
    params.keySet.asJava

  /** Response associated with request */
  lazy val response: Response = Response(this)

  /** Get response associated with request. */
  def getResponse(): Response = response

  /** Encode an HTTP message to String */
  def encodeString(): String = {
    new String(encodeBytes(), "UTF-8")
  }

  /** Encode an HTTP message to Array[Byte] */
  def encodeBytes(): Array[Byte] = {
    val encoder = new EncoderEmbedder[ChannelBuffer](new HttpRequestEncoder)
    encoder.offer(from[Request, HttpRequest](this))
    val buffer = encoder.poll()
    val bytes = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(bytes)
    bytes
  }

  override def toString: String =
    "Request(\"" + method + " " + uri + "\", from " + remoteSocketAddress + ")"
}


object Request {

  /**
   * [[com.twitter.collection.RecordSchema RecordSchema]] declaration, used
   * to generate [[com.twitter.collection.RecordSchema.Record Record]] instances
   * for Request.ctx.
   */
  val Schema: RecordSchema = new RecordSchema

  /** Decode a Request from a String */
  def decodeString(s: String): Request = {
    decodeBytes(s.getBytes(Charsets.Utf8))
  }

  /** Decode a Request from Array[Byte] */
  def decodeBytes(b: Array[Byte]): Request = {
    val decoder = new DecoderEmbedder(
      new HttpRequestDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue))
    decoder.offer(ChannelBuffers.wrappedBuffer(b))
    val req = decoder.poll().asInstanceOf[HttpRequest]
    assert(req ne null)
    new Request {
      val httpRequest = req
      lazy val remoteSocketAddress = new InetSocketAddress(0)
    }
  }

  /**
   * Create an HTTP/1.1 GET Request from query string parameters.
   *
   * @param params a list of key-value pairs representing the query string.
   */
  @varargs
  def apply(params: Tuple2[String, String]*): Request =
    apply("/", params:_*)

  /**
   * Create an HTTP/1.1 GET Request from URI and query string parameters.
   *
   * @param params a list of key-value pairs representing the query string.
   */
  def apply(uri: String, params: Tuple2[String, String]*): Request = {
    val encoder = new QueryStringEncoder(uri)
    params.foreach { case (key, value) =>
      encoder.addParam(key, value)
    }
    apply(Method.Get, encoder.toString)
  }

  /**
   * Create an HTTP/1.1 GET Request from URI string.
   * */
  def apply(uri: String): Request =
    apply(Method.Get, uri)

  /**
   * Create an HTTP/1.1 GET Request from method and URI string.
   */
  def apply(method: Method, uri: String): Request =
    apply(Version.Http11, method, uri)

  /**
   * Create an HTTP/1.1 GET Request from version, method, and URI string.
   */
  def apply(version: Version, method: Method, uri: String): Request = {
    val reqIn = new DefaultHttpRequest(from(version), from(method), uri)
    new Request {
      val httpRequest = reqIn
      lazy val remoteSocketAddress = new InetSocketAddress(0)
    }
  }

  /**
   * Create an HTTP/1.1 GET Request from Version, Method, URI, and Reader.
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
  def apply(
    version: Version,
    method: Method,
    uri: String,
    reader: Reader
  ): Request = {
    val httpReq = new DefaultHttpRequest(from(version), from(method), uri)
    httpReq.setChunked(true)
    apply(httpReq, reader, new InetSocketAddress(0))
  }

  private[http] def apply(
    reqIn: HttpRequest,
    readerIn: Reader,
    remoteAddr: InetSocketAddress
  ): Request = new Request {
    override val reader = readerIn
    val httpRequest = reqIn
    lazy val remoteSocketAddress = remoteAddr
  }

  /** Create Request from HttpRequest and Channel.  Used by Codec. */
  private[finagle] def apply(httpRequestArg: HttpRequest, channel: Channel): Request =
    new Request {
      val httpRequest = httpRequestArg
      lazy val remoteSocketAddress = channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
    }

  /** Create a query string from URI and parameters. */
  def queryString(uri: String, params: Tuple2[String, String]*): String = {
    val encoder = new QueryStringEncoder(uri)
    params.foreach { case (key, value) =>
      encoder.addParam(key, value)
    }
    encoder.toString
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
}
