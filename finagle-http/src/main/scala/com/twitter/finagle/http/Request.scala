package com.twitter.finagle.http

import com.google.common.base.Charsets
import com.twitter.finagle.http.netty.HttpRequestProxy
import java.net.{InetAddress, InetSocketAddress}
import java.io.ByteArrayOutputStream
import java.util.{AbstractMap, List => JList, Map => JMap, Set => JSet}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.Channel
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.http._
import scala.collection.JavaConverters._
import scala.reflect.BeanProperty


/**
 * Rich HttpRequest.
 *
 * Use RequestProxy to created an even richer subclass.
 */
abstract class Request extends Message with HttpRequestProxy {

  def isRequest = true

  lazy val params: ParamMap = new RequestParamMap(this)

  def method: HttpMethod           = getMethod
  def method_=(method: HttpMethod) = setMethod(method)
  def uri: String                  = getUri()
  def uri_=(uri: String)           { setUri(uri) }

  /** Path from URI. */
  @BeanProperty
  def path: String = {
    val u = getUri
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
    encoder.offer(this)
    val buffer = encoder.poll()
    val bytes = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(bytes)
    bytes
  }

  override def toString =
    "Request(\"" + method + " " + uri + "\", from " + remoteSocketAddress + ")"
}


object Request {

  /** Decode a Request from a String */
  def decodeString(s: String): Request = {
    decodeBytes(s.getBytes("UTF-8"))
  }

  /** Decode a Request from Array[Byte] */
  def decodeBytes(b: Array[Byte]): Request = {
    val decoder = new DecoderEmbedder(
      new HttpRequestDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue))
    decoder.offer(ChannelBuffers.wrappedBuffer(b))
    val httpRequest = decoder.poll().asInstanceOf[HttpRequest]
    assert(httpRequest ne null)
    Request(httpRequest)
  }

  /** Create Request from parameters.  Convenience method for testing. */
  def apply(params: Tuple2[String, String]*): MockRequest =
    apply("/", params:_*)

  /** Create Request from URI and parameters.  Convenience method for testing. */
  def apply(uri: String, params: Tuple2[String, String]*): MockRequest = {
    val encoder = new QueryStringEncoder(uri)
    params.foreach { case (key, value) =>
      encoder.addParam(key, value)
    }
    apply(Method.Get, encoder.toString)
  }

  /** Create Request from URI string.  Convenience method for testing. */
  def apply(uri: String): MockRequest =
    apply(Method.Get, uri)

  /** Create Request from method and URI string.  Convenience method for testing. */
  def apply(method: HttpMethod, uri: String): MockRequest =
    apply(Version.Http11, method, uri)

  /** Create Request from version, method, and URI string.  Convenience method for testing. */
  def apply(version: HttpVersion, method: HttpMethod, uri: String): MockRequest =
    apply(new DefaultHttpRequest(version, method, uri))

  /** Create Request from HttpRequest. */
  def apply(httpRequestArg: HttpRequest): MockRequest = {
    new MockRequest {
      override val httpRequest = httpRequestArg
      override val httpMessage = httpRequestArg
      override val remoteSocketAddress = new InetSocketAddress("127.0.0.1", 12345)
    }
  }

  /** Create Request from HttpRequest and Channel.  Used by Codec. */
  def apply(httpRequestArg: HttpRequest, channel: Channel): Request =
    new Request {
      val httpRequest = httpRequestArg
      override val httpMessage = httpRequestArg
      lazy val remoteSocketAddress = channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
    }

  // for testing
  protected class MockRequest extends Request {
    val httpRequest: HttpRequest = new DefaultHttpRequest(Version.Http11, Method.Get, "/")
    override val httpMessage: HttpMessage = httpRequest
    val remoteSocketAddress = new InetSocketAddress("127.0.0.1", 12345)

    // Create a MockRequest with a specific IP
    def withIp(ip: String) =
      new MockRequest {
        override val httpRequest = MockRequest.this
        override final val httpMessage = MockRequest.this
        override val remoteSocketAddress = new InetSocketAddress(ip, 12345)
      }

    // Create an internal MockRequest
    def internal = withIp("10.0.0.1")

    // Create an external MockRequest
    def external = withIp("8.8.8.8")
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
