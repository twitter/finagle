package com.twitter.finagle.http

import com.twitter.finagle.netty4.http.Netty4FormPostEncoder
import com.twitter.io.Buf
import com.twitter.util.Base64StringEncoder
import java.net.{URL, URI => JURI}
import java.nio.charset.StandardCharsets
import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

/**
 * Provides a class for building [[Request]]s.
 * The main class to use is [[com.twitter.finagle.http.RequestBuilder]], as so
 *
 * {{{
 * val getRequest = RequestBuilder()
 *   .setHeader(Fields.USER_AGENT, "MyBot")
 *   .setHeader(Fields.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
 *   .url(new URL("https://www.example.com"))
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
 * Request getRequest =
 *   RequestBuilder.safeBuildGet(
 *     RequestBuilder.create()
 *       .setHeader(Fields.USER_AGENT, "MyBot")
 *       .setHeader(Fields.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
 *       .url(new URL("https://www.example.com")))
 * }}}
 *
 * Overall RequestBuilder's pretty barebones. It does provide certain protocol level support
 * for more involved requests. For example, it supports easy creation of POST requests to submit
 * multipart web forms with `buildMultipartPost` and default form posts with `buildFormPost`.
 */
/**
 * Factory for [[com.twitter.finagle.http.RequestBuilder]] instances
 */
object RequestBuilder {

  private val SchemeAllowlist = Seq("http", "https")

  /** Evidence type that signifies that a property has been satisfied */
  sealed trait Valid

  @implicitNotFound(
    "Http RequestBuilder is not correctly configured: HasUrl (exp: Yes): ${HasUrl}, HasForm (exp: Nothing) ${HasForm}."
  )
  trait RequestEvidence[HasUrl, HasForm]
  object RequestEvidence {
    implicit object FullyConfigured extends RequestEvidence[Valid, Nothing]
  }

  @implicitNotFound(
    "Http RequestBuilder is not correctly configured for form post: HasUrl (exp: Yes): ${HasUrl}, HasForm (exp: Yes): ${HasForm}."
  )
  trait PostRequestEvidence[HasUrl, HasForm]
  object PostRequestEvidence {
    implicit object FullyConfigured extends PostRequestEvidence[Valid, Valid]
  }

  type Complete = RequestBuilder[Valid, Nothing]
  type CompleteForm = RequestBuilder[Valid, Valid]

  def apply() = new RequestBuilder()

  /**
   * Used for Java access.
   */
  def create() = apply()

  /**
   * Provides a typesafe `build` with content for Java.
   */
  def safeBuild(builder: Complete, method: Method, content: Option[Buf]): Request =
    builder.build(method, content)(RequestEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildGet` for Java.
   */
  def safeBuildGet(builder: Complete): Request =
    builder.buildGet()(RequestEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildHead` for Java.
   */
  def safeBuildHead(builder: Complete): Request =
    builder.buildHead()(RequestEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildDelete` for Java.
   */
  def safeBuildDelete(builder: Complete): Request =
    builder.buildDelete()(RequestEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildPut` for Java.
   */
  def safeBuildPut(builder: Complete, content: Buf): Request =
    builder.buildPut(content)(RequestEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildPost` for Java.
   */
  def safeBuildPost(builder: Complete, content: Buf): Request =
    builder.buildPost(content)(RequestEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildFormPost` for Java.
   */
  def safeBuildFormPost(builder: CompleteForm, multipart: Boolean): Request =
    builder.buildFormPost(multipart)(PostRequestEvidence.FullyConfigured)
}

class RequestBuilder[HasUrl, HasForm] private[http] (config: RequestConfig) {
  import RequestBuilder._

  type This = RequestBuilder[HasUrl, HasForm]

  private[http] def this() = this(RequestConfig())

  /*
   * Specify url as String
   */
  def url(u: String): RequestBuilder[Valid, HasForm] = url(new java.net.URL(u))

  /**
   * Specify the url to request. Sets the HOST header and possibly
   * the Authorization header using the authority portion of the URL.
   */
  def url(u: URL): RequestBuilder[Valid, HasForm] = {
    require(SchemeAllowlist.contains(u.getProtocol), s"url must be http(s), was ${u.getProtocol}")
    val uri = u.toURI
    val host = hostString(uri, u)
    val hostValue =
      if (u.getPort == -1 || u.getDefaultPort == u.getPort)
        host
      else
        "%s:%d".format(host, u.getPort)
    val withHost = config.headers.updated(Fields.Host, Seq(hostValue))
    val userInfo = uri.getUserInfo
    val updated =
      if (userInfo == null || userInfo.isEmpty)
        withHost
      else {
        val auth = "Basic " + Base64StringEncoder.encode(userInfo.getBytes(StandardCharsets.UTF_8))
        withHost.updated(Fields.Authorization, Seq(auth))
      }
    new RequestBuilder(config.copy(url = Some(u), headers = updated))
  }

  /*
   * Add simple form name/value pairs. In this mode, this RequestBuilder will only
   * be able to generate a multipart/form POST request.
   */
  def addFormElement(kv: (String, String)*): RequestBuilder[HasUrl, Valid] = {
    val elems = config.formElements
    val updated = kv.foldLeft(elems) { case (es, (k, v)) => es :+ SimpleElement(k, v) }
    new RequestBuilder(config.copy(formElements = updated))
  }

  /*
   * Add a FormElement to a request. In this mode, this RequestBuilder will only
   * be able to generate a multipart/form POST request.
   */
  def add(elem: FormElement): RequestBuilder[HasUrl, Valid] = {
    val elems = config.formElements
    val updated = elems ++ Seq(elem)
    new RequestBuilder(config.copy(formElements = updated))
  }

  /*
   * Add a group of FormElements to a request. In this mode, this RequestBuilder will only
   * be able to generate a multipart/form POST request.
   */
  def add(elems: Seq[FormElement]): RequestBuilder[HasUrl, Valid] = {
    val first = this.add(elems.head)
    elems.tail.foldLeft(first) { (b, elem) => b.add(elem) }
  }

  /**
   * Declare the HTTP protocol version be HTTP/1.0
   */
  def http10(): This =
    new RequestBuilder(config.copy(version = Version.Http10))

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
    setHeader(name, values.asScala.toSeq)
  }

  /**
   * Add a new header with the specified name and value.
   */
  def addHeader(name: String, value: String): This = {
    val values = config.headers.getOrElse(name, Seq.empty)
    val updated = config.headers.updated(name, values ++ Seq(value))
    new RequestBuilder(config.copy(headers = updated))
  }

  /**
   * Add group of headers expressed as a Map
   */
  def addHeaders(headers: Map[String, String]): This = {
    headers.foldLeft(this) { case (b, (k, v)) => b.addHeader(k, v) }
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
    val headers: SortedMap[String, Seq[String]] = credentials map { creds =>
      config.headers.updated(Fields.ProxyAuthorization, Seq(creds.basicAuthorization))
    } getOrElse config.headers

    new RequestBuilder(config.copy(headers = headers, proxied = true))
  }

  /**
   * Construct an HTTP request with a specified method.
   */
  def build(
    method: Method,
    content: Option[Buf]
  )(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[
      HasUrl,
      HasForm
    ]
  ): Request = {
    content match {
      case Some(ct) => withContent(method, ct)
      case None => withoutContent(method)
    }
  }

  /**
   * Construct an HTTP GET request.
   */
  def buildGet(
  )(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[
      HasUrl,
      HasForm
    ]
  ): Request = withoutContent(Method.Get)

  /**
   * Construct an HTTP HEAD request.
   */
  def buildHead(
  )(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[
      HasUrl,
      HasForm
    ]
  ): Request = withoutContent(Method.Head)

  /**
   * Construct an HTTP DELETE request.
   */
  def buildDelete(
  )(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[
      HasUrl,
      HasForm
    ]
  ): Request = withoutContent(Method.Delete)

  /**
   * Construct an HTTP POST request.
   */
  def buildPost(
    content: Buf
  )(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[
      HasUrl,
      HasForm
    ]
  ): Request = withContent(Method.Post, content)

  /**
   * Construct an HTTP PUT request.
   */
  def buildPut(
    content: Buf
  )(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[
      HasUrl,
      HasForm
    ]
  ): Request = withContent(Method.Put, content)

  /**
   * Construct a form post request.
   */
  def buildFormPost(
    multipart: Boolean = false
  )(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.PostRequestEvidence[
      HasUrl,
      HasForm
    ]
  ): Request = Netty4FormPostEncoder.encode(config, multipart)

  /**
   * Determine a host string from the given URI falling back to the URL
   * if the URI returns a null. We ideally want the host formatted in the way
   * URI formats the host string. However, if URI returns a null for the host
   * we return the host defined in the original URL. Note: the URL should NOT
   * be used for other operations, like equality which does a blocking DNS
   * resolution on the hostname.
   * @param uri - [[java.net.URI]] to use as a basis for determining the host
   * @param url - [[java.net.URL]] to use as a fallback for when the URI.getHost returns a null.
   * @see [[java.net.URI#getHost]]
   * @see [[java.net.URL#getHost]]
   *
   * @return lowercase string representation of the host for the request.
   */
  private[this] def hostString(uri: JURI, url: URL): String = {
    (if (uri.getHost == null) {
       // fallback to URL
       url.getHost
     } else {
       uri.getHost
     }).toLowerCase
  }

  private[http] def withoutContent(method: Method): Request = {
    val req = Request(config.version, method, RequestConfig.resource(config))

    config.headers.foreach {
      case (field, values) =>
        values.foreach { v => req.headerMap.add(field, v) }
    }

    req
  }

  private[http] def withContent(method: Method, content: Buf): Request = {
    require(content != null)
    val req = withoutContent(method)
    req.content = content
    req.headerMap.setUnsafe(Fields.ContentLength, content.length.toString)
    req
  }
}
