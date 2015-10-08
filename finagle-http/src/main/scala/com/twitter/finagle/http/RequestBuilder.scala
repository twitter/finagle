package com.twitter.finagle.http

import com.twitter.finagle.http.netty.Bijections
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.util.Base64StringEncoder
import com.twitter.io.Buf
import java.net.URL
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http.multipart.{DefaultHttpDataFactory, HttpPostRequestEncoder, HttpDataFactory}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpHeaders, HttpVersion, HttpMethod, DefaultHttpRequest}
import scala.annotation.implicitNotFound
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import Bijections._

/*
 * HTML form element.
 */
sealed abstract class FormElement

/*
 * HTML form simple input field.
 */
case class SimpleElement(name: String, content: String) extends FormElement

/*
 * HTML form file input field.
 */
case class FileElement(name: String, content: Buf, contentType: Option[String] = None,
  filename: Option[String] = None) extends FormElement

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
 *
 * Overall RequestBuilder's pretty barebones. It does provide certain protocol level support
 * for more involved requests. For example, it support easy creation of POST request to submit
 * multipart web forms with `buildMultipartPost` and default form post with `buildFormPost`.
 */

/**
 * Factory for [[com.twitter.finagle.http.RequestBuilder]] instances
 */
object RequestBuilder {
  @implicitNotFound("Http RequestBuilder is not correctly configured: HasUrl (exp: Yes): ${HasUrl}, HasForm (exp: Nothing) ${HasForm}.")
  private trait RequestEvidence[HasUrl, HasForm]
  private object RequestEvidence {
    implicit object FullyConfigured extends RequestEvidence[RequestConfig.Yes, Nothing]
  }

  @implicitNotFound("Http RequestBuilder is not correctly configured for form post: HasUrl (exp: Yes): ${HasUrl}, HasForm (exp: Yes): ${HasForm}.")
  private trait PostRequestEvidence[HasUrl, HasForm]
  private object PostRequestEvidence {
    implicit object FullyConfigured extends PostRequestEvidence[RequestConfig.Yes, RequestConfig.Yes]
  }

  type Complete = RequestBuilder[RequestConfig.Yes, Nothing]
  type CompleteForm = RequestBuilder[RequestConfig.Yes, RequestConfig.Yes]

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

object RequestConfig {
  sealed abstract trait Yes

  type FullySpecifiedConfig = RequestConfig[Yes, Nothing]
  type FullySpecifiedConfigForm = RequestConfig[Yes, Yes]
}

private[http] final case class RequestConfig[HasUrl, HasForm](
  url: Option[URL]                  = None,
  headers: Map[String, Seq[String]] = Map.empty,
  formElements: Seq[FormElement]    = Nil,
  version: Version                  = Version.Http11,
  proxied: Boolean                  = false
)

class RequestBuilder[HasUrl, HasForm] private[http](
  config: RequestConfig[HasUrl, HasForm]
) {
  import RequestConfig._

  type This = RequestBuilder[HasUrl, HasForm]

  private[this] val SCHEME_WHITELIST = Seq("http","https")

  private[http] def this() = this(RequestConfig())

  /*
   * Specify url as String
   */
  def url(u: String): RequestBuilder[Yes, HasForm] = url(new java.net.URL(u))

  /**
   * Specify the url to request. Sets the HOST header and possibly
   * the Authorization header using the authority portion of the URL.
   */
  def url(u: URL): RequestBuilder[Yes, HasForm] = {
    require(SCHEME_WHITELIST.contains(u.getProtocol), "url must be http(s)")
    val uri = u.toURI
    val host = uri.getHost.toLowerCase
    val hostValue =
      if (u.getPort == -1 || u.getDefaultPort == u.getPort)
        host
      else
        "%s:%d".format(host, u.getPort)
    val withHost = config.headers.updated(HttpHeaders.Names.HOST, Seq(hostValue))
    val userInfo =  uri.getUserInfo
    val updated =
      if (userInfo == null || userInfo.isEmpty)
        withHost
      else {
        val auth = "Basic " + Base64StringEncoder.encode(userInfo.getBytes)
        withHost.updated(HttpHeaders.Names.AUTHORIZATION, Seq(auth))
      }
    new RequestBuilder(config.copy(url = Some(u), headers = updated))
  }

  /*
   * Add simple form name/value pairs. In this mode, this RequestBuilder will only
   * be able to generate a multipart/form POST request.
   */
   def addFormElement(kv: (String, String)*): RequestBuilder[HasUrl, Yes] = {
     val elems = config.formElements
     val updated = kv.foldLeft(elems) { case (es, (k, v)) => es :+ new SimpleElement(k, v) }
     new RequestBuilder(config.copy(formElements = updated))
   }

  /*
   * Add a FormElement to a request. In this mode, this RequestBuilder will only
   * be able to generate a multipart/form POST request.
   */
  def add(elem: FormElement): RequestBuilder[HasUrl, Yes] = {
    val elems = config.formElements
    val updated = elems ++ Seq(elem)
    new RequestBuilder(config.copy(formElements = updated))
  }

  /*
   * Add a group of FormElements to a request. In this mode, this RequestBuilder will only
   * be able to generate a multipart/form POST request.
   */
  def add(elems: Seq[FormElement]): RequestBuilder[HasUrl, Yes] = {
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
    val headers: Map[String,Seq[String]] = credentials map { creds =>
      config.headers.updated(HttpHeaders.Names.PROXY_AUTHORIZATION, Seq(creds.basicAuthorization))
    } getOrElse config.headers

    new RequestBuilder(config.copy(headers = headers, proxied = true))
  }

  /**
   * Construct an HTTP request with a specified method.
   */
  def build(method: Method, content: Option[Buf])(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[HasUrl, HasForm]
  ): Request = {
    content match {
      case Some(content) => withContent(method, content)
      case None => withoutContent(method)
    }
  }

  /**
   * Construct an HTTP GET request.
   */
  def buildGet()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[HasUrl, HasForm]
  ): Request = withoutContent(Method.Get)

  /**
   * Construct an HTTP HEAD request.
   */
  def buildHead()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[HasUrl, HasForm]
  ): Request = withoutContent(Method.Head)

  /**
   * Construct an HTTP DELETE request.
   */
  def buildDelete()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[HasUrl, HasForm]
  ): Request = withoutContent(Method.Delete)

  /**
   * Construct an HTTP POST request.
   */
  def buildPost(content: Buf)(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[HasUrl, HasForm]
  ): Request = withContent(Method.Post, content)

  /**
   * Construct an HTTP PUT request.
   */
  def buildPut(content: Buf)(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.RequestEvidence[HasUrl, HasForm]
  ): Request = withContent(Method.Put, content)

  /**
   * Construct a form post request.
   */
  def buildFormPost(multipart: Boolean = false) (
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: RequestBuilder.PostRequestEvidence[HasUrl, HasForm]
  ): Request = {
    val dataFactory = new DefaultHttpDataFactory(false) // we don't use disk
    val req = withoutContent(Method.Post)
    val encoder = new HttpPostRequestEncoder(dataFactory, req.httpRequest, multipart)

    config.formElements.foreach {
      case FileElement(name, content, contentType, filename) =>
        HttpPostRequestEncoderEx.addBodyFileUpload(encoder, dataFactory, req.httpRequest)(
          name, filename.getOrElse(""),
          BufChannelBuffer(content),
          contentType.getOrElse(null),
          false)

      case SimpleElement(name, value) =>
        encoder.addBodyAttribute(name, value)
    }
    val encodedReq = encoder.finalizeRequest()

    if (encodedReq.isChunked) {
      val encodings = encodedReq.headers.getAll(HttpHeaders.Names.TRANSFER_ENCODING)
      encodings.remove(HttpHeaders.Values.CHUNKED)
      if (encodings.isEmpty)
        encodedReq.headers.remove(HttpHeaders.Names.TRANSFER_ENCODING)
      else
        encodedReq.headers.set(HttpHeaders.Names.TRANSFER_ENCODING, encodings)

      val chunks = new ListBuffer[ChannelBuffer]
      while (encoder.hasNextChunk) {
        chunks += encoder.nextChunk().getContent()
      }
      encodedReq.setContent(ChannelBuffers.wrappedBuffer(chunks:_*))
    }

    from(encodedReq)
  }

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

  private[http] def withoutContent(method: Method): Request = {
    val req = Request(config.version, method, resource)
    config.headers foreach { case (field, values) =>
      values foreach { v => req.headers.add(field, v) }
    }
    req
  }

  private[http] def withContent(method: Method, content: Buf): Request = {
    require(content != null)
    val req = withoutContent(method)
    req.content = content
    req.headers.set(HttpHeaders.Names.CONTENT_LENGTH, content.length.toString)
    req
  }
}

/**
 * Add a missing method to HttpPostRequestEncoder to allow specifying a ChannelBuffer directly as
 * content of a file. This logic should eventually move to netty.
 */
private object HttpPostRequestEncoderEx {
  //TODO: HttpPostBodyUtil not accessible from netty 3.5.0.Final jar
  //      This HttpPostBodyUtil simulates what we need.
  object HttpPostBodyUtil {
    val DEFAULT_TEXT_CONTENT_TYPE = "text/plain"
    val DEFAULT_BINARY_CONTENT_TYPE = "application/octet-stream"
    object TransferEncodingMechanism {
      val BINARY = "binary"
      val BIT7 = "7bit"
    }
  }

  /*
   * allow specifying post body as ChannelBuffer, the logic is adapted from netty code.
   */
  def addBodyFileUpload(encoder: HttpPostRequestEncoder, factory: HttpDataFactory, request: HttpRequest)
    (name: String, filename: String, content: ChannelBuffer, contentType: String, isText: Boolean) {
    require(name != null)
    require(filename != null)
    require(content != null)

    val scontentType =
      if (contentType == null) {
        if (isText) {
          HttpPostBodyUtil.DEFAULT_TEXT_CONTENT_TYPE
        } else {
          HttpPostBodyUtil.DEFAULT_BINARY_CONTENT_TYPE
        }
      } else {
        contentType
      }
    val contentTransferEncoding =
      if (!isText) {
        HttpPostBodyUtil.TransferEncodingMechanism.BINARY
      } else {
        HttpPostBodyUtil.TransferEncodingMechanism.BIT7
      }

    val fileUpload = factory.createFileUpload(request, name, filename, scontentType, contentTransferEncoding, null, content.readableBytes)
    fileUpload.setContent(content)
    encoder.addBodyHttpData(fileUpload)
  }
}
