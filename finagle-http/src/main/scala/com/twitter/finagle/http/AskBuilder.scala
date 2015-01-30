package com.twitter.finagle.http

import com.twitter.util.Base64StringEncoder
import java.net.URL
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http.multipart.{DefaultHttpDataFactory, HttpPostRequestEncoder => HttpPostAskEncoder, HttpDataFactory}
import org.jboss.netty.handler.codec.http.{HttpRequest => HttpAsk, HttpHeaders, HttpVersion, HttpMethod, DefaultHttpRequest => DefaultHttpAsk}
import scala.annotation.implicitNotFound
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

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
case class FileElement(name: String, content: ChannelBuffer, contentType: Option[String] = None,
  filename: Option[String] = None) extends FormElement

/**
 * Provides a class for building [[org.jboss.netty.handler.codec.http.HttpAsk]]s.
 * The main class to use is [[com.twitter.finagle.http.AskBuilder]], as so
 *
 * {{{
 * val getAsk = AskBuilder()
 *   .setHeader(HttpHeaders.Names.USER_AGENT, "MyBot")
 *   .setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
 *   .url(new URL("http://www.example.com"))
 *   .buildGet()
 * }}}
 *
 * The `AskBuilder` requires the definition of `url`. In Scala,
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
 * HttpAsk getAsk =
 *   AskBuilder.safeBuildGet(
 *     AskBuilder.create()
 *       .setHeader(HttpHeaders.Names.USER_AGENT, "MyBot")
 *       .setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
 *       .url(new URL("http://www.example.com")))
 * }}}
 *
 * Overall AskBuilder's pretty barebones. It does provide certain protocol level support
 * for more involved requests. For example, it support easy creation of POST request to submit
 * multipart web forms with `buildMultipartPost` and default form post with `buildFormPost`.
 */

/**
 * Factory for [[com.twitter.finagle.http.AskBuilder]] instances
 */
object AskBuilder {
  @implicitNotFound("Http AskBuilder is not correctly configured: HasUrl (exp: Yes): ${HasUrl}, HasForm (exp: Nothing) ${HasForm}.")
  private trait AskEvidence[HasUrl, HasForm]
  private object AskEvidence {
    implicit object FullyConfigured extends AskEvidence[AskConfig.Yes, Nothing]
  }

  @implicitNotFound("Http AskBuilder is not correctly configured for form post: HasUrl (exp: Yes): ${HasUrl}, HasForm (exp: Yes): ${HasForm}.")
  private trait PostAskEvidence[HasUrl, HasForm]
  private object PostAskEvidence {
    implicit object FullyConfigured extends PostAskEvidence[AskConfig.Yes, AskConfig.Yes]
  }

  type Complete = AskBuilder[AskConfig.Yes, Nothing]
  type CompleteForm = AskBuilder[AskConfig.Yes, AskConfig.Yes]

  def apply() = new AskBuilder()

  /**
   * Used for Java access.
   */
  def create() = apply()

  /**
   * Provides a typesafe `build` with content for Java.
   */
  def safeBuild(builder: Complete, method: HttpMethod, content: Option[ChannelBuffer]): HttpAsk =
    builder.build(method, content)(AskEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildGet` for Java.
   */
  def safeBuildGet(builder: Complete): HttpAsk =
    builder.buildGet()(AskEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildHead` for Java.
   */
  def safeBuildHead(builder: Complete): HttpAsk =
    builder.buildHead()(AskEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildDelete` for Java.
   */
  def safeBuildDelete(builder: Complete): HttpAsk =
    builder.buildDelete()(AskEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildPut` for Java.
   */
  def safeBuildPut(builder: Complete, content: ChannelBuffer): HttpAsk =
    builder.buildPut(content)(AskEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildPut` for Java.
   */
  @deprecated("Typo, use safeBuildPut instead", "5.3.7")
  def safeBuidlPut(builder: Complete, content: ChannelBuffer): HttpAsk =
    safeBuildPut(builder, content)

  /**
   * Provides a typesafe `buildPost` for Java.
   */
  def safeBuildPost(builder: Complete, content: ChannelBuffer): HttpAsk =
    builder.buildPost(content)(AskEvidence.FullyConfigured)

  /**
   * Provides a typesafe `buildFormPost` for Java.
   */
  def safeBuildFormPost(builder: CompleteForm, multipart: Boolean): HttpAsk =
    builder.buildFormPost(multipart)(PostAskEvidence.FullyConfigured)
}

object AskConfig {
  sealed abstract trait Yes

  type FullySpecifiedConfig = AskConfig[Yes, Nothing]
  type FullySpecifiedConfigForm = AskConfig[Yes, Yes]
}

private[http] final case class AskConfig[HasUrl, HasForm](
  url: Option[URL]                 = None,
  headers: Map[String,Seq[String]] = Map(),
  formElements: Seq[FormElement]   = Seq(),
  version: HttpVersion             = HttpVersion.HTTP_1_1,
  proxied: Boolean                 = false
)

class AskBuilder[HasUrl, HasForm] private[http](
  config: AskConfig[HasUrl, HasForm]
) {
  import AskConfig._

  type This = AskBuilder[HasUrl, HasForm]

  private[this] val SCHEME_WHITELIST = Seq("http","https")

  private[http] def this() = this(AskConfig())

  /*
   * Specify url as String
   */
  def url(u: String): AskBuilder[Yes, HasForm] = url(new java.net.URL(u))

  /**
   * Specify the url to request. Sets the HOST header and possibly
   * the Authorization header using the authority portion of the URL.
   */
  def url(u: URL): AskBuilder[Yes, HasForm] = {
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
    new AskBuilder(config.copy(url = Some(u), headers = updated))
  }

  /*
   * Add simple form name/value pairs. In this mode, this AskBuilder will only
   * be able to generate a multipart/form POST request.
   */
   def addFormElement(kv: (String, String)*): AskBuilder[HasUrl, Yes] = {
     val elems = config.formElements
     val updated = kv.foldLeft(elems) { case (es, (k, v)) => es :+ new SimpleElement(k, v) }
     new AskBuilder(config.copy(formElements = updated))
   }

  /*
   * Add a FormElement to a request. In this mode, this AskBuilder will only
   * be able to generate a multipart/form POST request.
   */
  def add(elem: FormElement): AskBuilder[HasUrl, Yes] = {
    val elems = config.formElements
    val updated = elems ++ Seq(elem)
    new AskBuilder(config.copy(formElements = updated))
  }

  /*
   * Add a group of FormElements to a request. In this mode, this AskBuilder will only
   * be able to generate a multipart/form POST request.
   */
  def add(elems: Seq[FormElement]): AskBuilder[HasUrl, Yes] = {
    val first = this.add(elems.head)
    elems.tail.foldLeft(first) { (b, elem) => b.add(elem) }
  }

  /**
   * Declare the HTTP protocol version be HTTP/1.0
   */
  def http10(): This =
    new AskBuilder(config.copy(version = HttpVersion.HTTP_1_0))

  /**
   * Set a new header with the specified name and value.
   */
  def setHeader(name: String, value: String): This = {
    val updated = config.headers.updated(name, Seq(value))
    new AskBuilder(config.copy(headers = updated))
  }

  /**
   * Set a new header with the specified name and values.
   */
  def setHeader(name: String, values: Seq[String]): This = {
    val updated = config.headers.updated(name, values)
    new AskBuilder(config.copy(headers = updated))
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
    new AskBuilder(config.copy(headers = updated))
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

    new AskBuilder(config.copy(headers = headers, proxied = true))
  }

  /**
   * Construct an HTTP request with a specified method.
   */
  def build(method: HttpMethod, content: Option[ChannelBuffer])(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: AskBuilder.AskEvidence[HasUrl, HasForm]
  ): HttpAsk = {
    content match {
      case Some(content) => withContent(method, content)
      case None => withoutContent(method)
    }
  }

  /**
   * Construct an HTTP GET request.
   */
  def buildGet()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: AskBuilder.AskEvidence[HasUrl, HasForm]
  ): HttpAsk = withoutContent(HttpMethod.GET)

  /**
   * Construct an HTTP HEAD request.
   */
  def buildHead()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: AskBuilder.AskEvidence[HasUrl, HasForm]
  ): HttpAsk = withoutContent(HttpMethod.HEAD)

  /**
   * Construct an HTTP DELETE request.
   */
  def buildDelete()(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: AskBuilder.AskEvidence[HasUrl, HasForm]
  ): HttpAsk = withoutContent(HttpMethod.DELETE)

  /**
   * Construct an HTTP POST request.
   */
  def buildPost(content: ChannelBuffer)(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: AskBuilder.AskEvidence[HasUrl, HasForm]
  ): HttpAsk = withContent(HttpMethod.POST, content)

  /**
   * Construct an HTTP PUT request.
   */
  def buildPut(content: ChannelBuffer)(
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: AskBuilder.AskEvidence[HasUrl, HasForm]
  ): HttpAsk = withContent(HttpMethod.PUT, content)

  /**
   * Construct a form post request.
   */
  def buildFormPost(multipart: Boolean = false) (
    implicit HTTP_REQUEST_BUILDER_IS_NOT_FULLY_SPECIFIED: AskBuilder.PostAskEvidence[HasUrl, HasForm]
  ): HttpAsk = {
    val dataFactory = new DefaultHttpDataFactory(false) // we don't use disk
    val req = withoutContent(HttpMethod.POST)
    val encoder = new HttpPostAskEncoder(dataFactory, req, multipart)

    config.formElements.foreach {
      case FileElement(name, content, contentType, filename) =>
        HttpPostAskEncoderEx.addBodyFileUpload(encoder, dataFactory, req)(name, filename.getOrElse(""), content, contentType.getOrElse(null), false)
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

    encodedReq
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

  private[http] def withoutContent(method: HttpMethod): HttpAsk = {
    val req = new DefaultHttpAsk(config.version, method, resource)
    config.headers.foreach { case (k,vs) =>
      vs.foreach { v =>
        req.headers.add(k, v)
      }
    }
    req
  }

  private[http] def withContent(method: HttpMethod, content: ChannelBuffer): HttpAsk = {
    require(content != null)
    val req = withoutContent(method)
    req.setContent(content)
    req.headers.set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes.toString)
    req
  }
}

/**
 * Add a missing method to HttpPostAskEncoder to allow specifying a ChannelBuffer directly as
 * content of a file. This logic should eventually move to netty.
 */
private object HttpPostAskEncoderEx {
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
  def addBodyFileUpload(encoder: HttpPostAskEncoder, factory: HttpDataFactory, request: HttpAsk)
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
