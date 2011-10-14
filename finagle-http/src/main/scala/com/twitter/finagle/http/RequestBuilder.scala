package com.twitter.finagle.http

/**
 * Provides a class for building [[org.jboss.netty.handler.codec.http.HttpRequest]]s.
 * The main class to use is [[com.twitter.finagle.http.RequestBuilder]], as so
 *
 * {{{
 * val getRequest = RequestBuilder()
 *   .setHeader(HttpHeaders.Names.USER_AGENT, "MyBot")
 *   .setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
 *   .url(new URL("http://www.example.com"))
 *   .buildGet()
 * }}}
 *
 * The `RequestBuilder` requires the definition of `url`. In Scala,
 * this is statically type checked, and in Java the lack of any of
 * a url causes a runtime error.
 *
 * The `buildGet`, 'buildHead`, `buildPut`, and `buildPost` methods use an implicit argument
 * to statically typecheck the builder (to ensure completeness, see above).
 * The Java compiler cannot provide such implicit, so we provide separate
 * functions in Java to accomplish this. Thus, the Java code for the
 * above is
 *
 * {{{
 * HttpRequest getRequest =
 *   RequestBuilder.safeBuildGet(
 *     RequestBuilder.create()
 *       .setHeader(HttpHeaders.Names.USER_AGENT, "MyBot")
 *       .setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
 *       .url(new URL("http://www.example.com")))
 * }}}
 */

import com.twitter.util.Base64StringEncoder
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpHeaders, HttpVersion, HttpMethod, DefaultHttpRequest}
import java.net.URL
import scala.collection.JavaConversions._

/**
 * Factory for [[com.twitter.finagle.http.RequestBuilder]] instances
 */
object RequestBuilder {
  type Complete = RequestBuilder[RequestConfig.Yes]

  def apply() = new RequestBuilder

  /**
   * Used for Java access.
   */
  def create() = apply()

  /**
   * Provides a typesafe `buildGet` for Java.
   */
  def safeBuildGet(builder: Complete): HttpRequest =
    builder.buildGet()

  /**
   * Provides a typesafe `buildHead` for Java.
   */
  def safeBuildHead(builder: Complete): HttpRequest =
    builder.buildHead()

  /**
   * Provides a typesafe `buildDelete` for Java.
   */
  def safeBuildDelete(builder: Complete): HttpRequest =
    builder.buildDelete()

  /**
   * Provides a typesafe `buildPut` for Java.
   */
  def safeBuidlPut(builder: Complete, content: ChannelBuffer): HttpRequest =
    builder.buildPut(content)

  /**
   * Provides a typesafe `buildPost` for Java.
   */
  def safeBuildPost(builder: Complete, content: ChannelBuffer): HttpRequest =
    builder.buildPost(content)
}

object RequestConfig {
  sealed abstract trait Yes
  type FullySpecifiedConfig = RequestConfig[Yes]
}

private[http] final case class RequestConfig[HasUrl](
  url: Option[URL]                 = None,
  headers: Map[String,Seq[String]] = Map(),
  version: HttpVersion             = HttpVersion.HTTP_1_1,
  proxied: Boolean                 = false
)


class RequestBuilder[HasUrl] private[http](
  config: RequestConfig[HasUrl]
) {
  import RequestConfig._

  type ThisConfig = RequestConfig[HasUrl]
  type This = RequestBuilder[HasUrl]

  private[this] val SCHEME_WHITELIST = Seq("http","https")

  private[http] def this() = this(RequestConfig())

  /**
   * Specify the url to request. Sets the HOST header and possibly
   * the Authorization header using the authority portion of the URL.
   */
  def url(u: URL): RequestBuilder[Yes] = {
    require(SCHEME_WHITELIST.contains(u.getProtocol), "url must be http(s)")
    val uri = u.toURI
    val host = uri.getHost.toLowerCase
    val hostValue =
      if (u.getPort == -1)
        host
      else
        "%s:%d".format(host, u.getPort)
    val withHost = config.headers.updated(HttpHeaders.Names.HOST, Seq(hostValue))
    val userInfo =  u.getUserInfo
    val updated =
      if (userInfo == null || userInfo.isEmpty)
        withHost
      else {
        val auth = "Basic " + Base64StringEncoder.encode(userInfo.getBytes)
        withHost.updated(HttpHeaders.Names.AUTHORIZATION, Seq(auth))
      }
    new RequestBuilder(config.copy(url = Some(u), headers = updated))
  }

  /**
   * Declare the HTTP protocol version be HTTP/1.0
   */
  def http10(): This =
    new RequestBuilder(config.copy(version = HttpVersion.HTTP_1_0))

  /**
   * Set a new header with the specified name and value.
   */
  def setHeader(name: String, value: String): This = {
    val updated = config.headers.updated(name, Seq(value))
    new RequestBuilder(config.copy(headers = updated))
  }

  /**
   * Set a new header with the specified name and values.
   */
  def setHeader(name: String, values: Seq[String]): This = {
    val updated = config.headers.updated(name, values)
    new RequestBuilder(config.copy(headers = updated))
  }

  /**
   * Set a new header with the specified name and values.
   *
   * Java convenience variant.
   */
  def setHeader(name: String, values: java.lang.Iterable[String]): This = {
    setHeader(name, values.toSeq)
  }

  /**
   * Add a new header with the specified name and value.
   */
  def addHeader(name: String, value: String): This = {
    val values = config.headers.get(name).getOrElse(Seq())
    val updated = config.headers.updated(
      name, values ++ Seq(value))
    new RequestBuilder(config.copy(headers = updated))
  }

  /**
   * Declare the request will be proxied. Results in using the
   * absolute URI in the request line.
   */
  def proxied(): This = proxied(None)

  /**
   * Declare the request will be proxied. Results in using the
   * absolute URI in the request line and setting the Proxy-Authorization
   * header using the provided {{ProxyCredentials}}.
   */
  def proxied(credentials: ProxyCredentials): This = proxied(Some(credentials))

  /**
   * Declare the request will be proxied. Results in using the
   * absolute URI in the request line and optionally setting the
   * Proxy-Authorization header using the provided {{ProxyCredentials}}.
   */
  def proxied(credentials: Option[ProxyCredentials]): This = {
    val headers: Map[String,Seq[String]] = credentials map { creds =>
      config.headers.updated(HttpHeaders.Names.PROXY_AUTHORIZATION, Seq(creds.basicAuthorization))
    } getOrElse config.headers

    new RequestBuilder(config.copy(headers = headers, proxied = true))
  }

  /**
   * Construct an HTTP GET request.
   */
  def buildGet()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: ThisConfig =:= FullySpecifiedConfig
  ): HttpRequest = withoutContent(HttpMethod.GET)

  /**
   * Construct an HTTP HEAD request.
   */
  def buildHead()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: ThisConfig =:= FullySpecifiedConfig
  ): HttpRequest = withoutContent(HttpMethod.HEAD)

  /**
   * Construct an HTTP DELETE request.
   */
  def buildDelete()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: ThisConfig =:= FullySpecifiedConfig
  ): HttpRequest = withoutContent(HttpMethod.DELETE)

  /**
   * Construct an HTTP POST request.
   */
  def buildPost(content: ChannelBuffer)(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: ThisConfig =:= FullySpecifiedConfig
  ): HttpRequest = withContent(HttpMethod.POST, content)

  /**
   * Construct an HTTP PUT request.
   */
  def buildPut(content: ChannelBuffer)(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: ThisConfig =:= FullySpecifiedConfig
  ): HttpRequest = withContent(HttpMethod.PUT, content)

  // absoluteURI if proxied, otherwise relativeURI
  private[this] def resource(): String = {
    val url = config.url.get
    if (config.proxied) {
      return url.toString
    } else {
      val builder = new StringBuilder()

      val path = url.getPath
      if (path == null || path.isEmpty)
        builder.append("/")
      else
        builder.append(path)

      val query = url.getQuery
      if (query != null && !query.isEmpty)
        builder.append("?%s".format(query))

      builder.toString
    }
  }

  private[this] def withoutContent(method: HttpMethod): HttpRequest = {
    val req = new DefaultHttpRequest(config.version, method, resource)
    config.headers.foreach { case (k,vs) =>
      vs.foreach { v =>
        req.addHeader(k, v)
      }
    }
    req
  }

  private[this] def withContent(method: HttpMethod, content: ChannelBuffer): HttpRequest = {
    val req = withoutContent(method)
    req.setContent(content)
    req
  }
}
