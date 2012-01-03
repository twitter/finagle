package com.twitter.finagle.http

import com.twitter.util.Duration
import java.io.{InputStream, InputStreamReader, OutputStream, OutputStreamWriter, Reader, Writer}
import java.util.{Iterator => JIterator}
import java.nio.charset.Charset
import java.util.{Date, TimeZone}
import org.apache.commons.lang.time.FastDateFormat
import org.jboss.netty.buffer._
import org.jboss.netty.handler.codec.http.{Cookie, HttpMessage, HttpHeaders, HttpMethod,
  HttpVersion}
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty


/**
 * Rich HttpMessage
 *
 * Base class for Request and Response.  There are both input and output
 * methods, though only one set of methods should be used.
 */
abstract class Message extends HttpMessage {

  def isRequest: Boolean
  def isResponse = !isRequest

  def content: ChannelBuffer = getContent()
  def content_=(content: ChannelBuffer) { setContent(content) }

  def version: HttpVersion = getProtocolVersion()
  def version_=(version: HttpVersion) { setProtocolVersion(version) }

  lazy val headers = new HeaderMap(this)
  // Java users: use Netty HttpMessage interface for headers

  /** Cookies.  In a request, this uses the Cookie headers.  In a response, it
    * uses the Set-Cookie headers. */
  lazy val cookies = new CookieSet(this)
  // Java users: use the interface below for cookies

  /** Get iterator over Cookies */
  def getCookies(): JIterator[Cookie] = cookies.iterator

  /** Add a cookie */
  def addCookie(cookie: Cookie) {
    cookies += cookie
  }

  /** Remove a cookie */
  def removeCookie(cookie: Cookie) {
    cookies -= cookie
  }

  /** Accept header */
  def accept: Seq[String] =
    Option(getHeader(HttpHeaders.Names.ACCEPT)) match {
      case Some(s) => s.split(",").map(_.trim).filter(_.nonEmpty)
      case None    => Seq()
    }
  /** Set Accept header */
  def accept_=(value: String) { setHeader(HttpHeaders.Names.ACCEPT, value) }
  /** Set Accept header with list of values */
  def accept_=(values: Iterable[String]) { accept = values.mkString(", ") }

  /** Accept header media types (normalized, no parameters) */
  def acceptMediaTypes: Seq[String] =
    accept.map {
      _.split(";", 2).headOption
         .map(_.trim.toLowerCase) // media types are case-insensitive
         .filter(_.nonEmpty)      // skip blanks
    }.flatten

  /** Allow header */
  def allow: Option[String] = Option(getHeader(HttpHeaders.Names.ALLOW))
  /** Set Authorization header */
  def allow_=(value: String) { setHeader(HttpHeaders.Names.ALLOW, value) }
  /** Set Authorization header */
  def allow_=(values: Iterable[HttpMethod]) { allow = values.mkString(",") }

  /** Get Authorization header */
  def authorization: Option[String] = Option(getHeader(HttpHeaders.Names.AUTHORIZATION))
  /** Set Authorization header */
  def authorization_=(value: String) { setHeader(HttpHeaders.Names.AUTHORIZATION, value) }

  /** Get Cache-Control header */
  def cacheControl: Option[String] = Option(getHeader(HttpHeaders.Names.CACHE_CONTROL))
  /** Set Cache-Control header */
  def cacheControl_=(value: String) { setHeader(HttpHeaders.Names.CACHE_CONTROL, value) }
  /** Set Cache-Control header with a max-age (and must-revalidate). */
  def cacheControl_=(maxAge: Duration) {
    cacheControl = "max-age=" + maxAge.inSeconds.toString + ", must-revalidate"
  }

  /** Get Content-Length header.  Use length to get the length of actual content. */
  def contentLength: Option[Long] =
    Option(getHeader(HttpHeaders.Names.CONTENT_LENGTH)).map { _.toLong }
  /** Set Content-Length header.  Normally, this is automatically set by the
    * Codec, but this method allows you to override that. */
  def contentLength_=(value: Long) {
    setHeader(HttpHeaders.Names.CONTENT_LENGTH, value.toString)
  }

  /** Get Content-Type header */
  def contentType: Option[String] = Option(getHeader(HttpHeaders.Names.CONTENT_TYPE))
  /** Set Content-Type header */
  def contentType_=(value: String) { setHeader(HttpHeaders.Names.CONTENT_TYPE, value) }
  /** Set Content-Type header by media-type and charset */
  def setContentType(mediaType: String, charset: String) {
    setHeader(HttpHeaders.Names.CONTENT_TYPE, mediaType + ";charset=" + charset)
  }
  /** Set Content-Type header to application/json;charset=utf-8 */
  def setContentTypeJson() { setHeader(HttpHeaders.Names.CONTENT_TYPE, Message.ContentTypeJson) }

  /** Get Expires header */
  def expires: Option[String] = Option(getHeader(HttpHeaders.Names.EXPIRES))
  /** Set Expires header */
  def expires_=(value: String) { setHeader(HttpHeaders.Names.EXPIRES, value) }
  /** Set Expires header by Date */
  def expires_=(value: Date) { expires = Message.httpDateFormat(value) }

  /** Get Host header */
  def host: Option[String] =  Option(getHeader(HttpHeaders.Names.HOST))
  /** Set Host header */
  def host_=(value: String) { setHeader(HttpHeaders.Names.HOST, value) }

  /** Get Location header */
  def location: Option[String] = Option(getHeader(HttpHeaders.Names.LOCATION))
  /** Set Location header */
  def location_=(value: String) { setHeader(HttpHeaders.Names.LOCATION, value) }

  /** Get media-type from Content-Type header */
  def mediaType: Option[String] =
    contentType.flatMap { contentType =>
      val beforeSemi =
        contentType.indexOf(";") match {
          case -1 => contentType
          case n  => contentType.substring(0, n)
        }
      val mediaType = beforeSemi.trim
      if (mediaType.nonEmpty)
        Some(mediaType.toLowerCase)
      else
        None
    }
  /** Set media-type in Content-Type heaer */
  def mediaType_=(value: String) { setContentType(value, "utf-8") }

  /** Get Referer [sic] header */
  def referer: Option[String] = Option(getHeader(HttpHeaders.Names.REFERER))
  /** Set Referer [sic] header */
  def referer_=(value: String) { setHeader(HttpHeaders.Names.REFERER, value) }

  /** Get Retry-After header */
  def retryAfter: Option[String] = Option(getHeader(HttpHeaders.Names.RETRY_AFTER))
  /** Set Retry-After header */
  def retryAfter_=(value: String) { setHeader(HttpHeaders.Names.RETRY_AFTER, value) }
  /** Set Retry-After header by seconds */
  def retryAfter_=(value: Long) { retryAfter = value.toString }

  /** Get User-Agent header */
  def userAgent: Option[String] = Option(getHeader(HttpHeaders.Names.USER_AGENT))
  /** Set User-Agent header */
  def userAgent_=(value: String) { setHeader(HttpHeaders.Names.USER_AGENT, value) }

  /** Get X-Forwarded-For header */
  def xForwardedFor: Option[String] = Option(getHeader("X-Forwarded-For"))
  /** Set X-Forwarded-For header */
  def xForwardedFor_=(value: String) { setHeader("X-Forwarded-For", value) }

  /** Get length of content. */
  def length: Int = getContent.readableBytes
  def getLength(): Int = length

  /** Get the content as a string. */
  def contentString: String = getContent.toString(Charset.forName("UTF-8"))
  def getContentString(): String = contentString

  /** Set the content as a string. */
  def contentString_=(value: String) {
    if (value != "")
      setContent(ChannelBuffers.wrappedBuffer(value.getBytes("UTF-8")))
    else
      setContent(ChannelBuffers.EMPTY_BUFFER)
  }
  def setContentString(value: String) { contentString = value }

  /**
   * Use content as InputStream.  The underlying channel buffer's reader
   * index is advanced.  (Scala interface.  Java users can use getInputStream().)
   */
  def withInputStream(f: (InputStream => Unit)) {
    val inputStream = getInputStream()
    f(inputStream) // throws
    inputStream.close()
  }

  /**
   * Get InputStream for content.  Caller must close.  (Java interface.  Scala
   * users should use withInputStream.)
   */
  def getInputStream(): InputStream =
    new ChannelBufferInputStream(getContent)

  /** Use content as Reader.  (Scala interface.  Java usrs can use getReader().) */
  def withReader(f: Reader => Unit) {
    withInputStream { inputStream =>
      val reader = new InputStreamReader(inputStream)
      f(reader)
    }
  }

  /** Get Reader for content.  (Java interface.  Scala users should use withReader.) */
  def getReader(): Reader =
    new InputStreamReader(getInputStream())

  /** Append string to content. */
  def write(string: String) {
    write(string.getBytes("UTF-8"))
  }

  /** Append bytes to content. */
  def write(bytes: Array[Byte]) {
    getContent match {
      case buffer: DynamicChannelBuffer =>
        buffer.writeBytes(bytes)
      case _ =>
        val buffer = ChannelBuffers.wrappedBuffer(bytes)
        write(buffer)
    }
  }

  /** Append ChannelBuffer to content. */
  def write(buffer: ChannelBuffer) {
    getContent match {
      case ChannelBuffers.EMPTY_BUFFER =>
        setContent(buffer)
      case content =>
        setContent(ChannelBuffers.wrappedBuffer(content, buffer))
    }
  }

  /**
   * Use content as OutputStream.  Content is replaced with stream contents.
   * (Java users can use this with a Function, or use Netty's ChannelBufferOutputStream
   * and then call setContent() with the underlying buffer.)
   */
  def withOutputStream(f: (OutputStream => Unit)) {
    // Use buffer size of 1024.  Netty default is 256, which seems too small.
    // Netty doubles buffers on resize.
    val outputStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(1024))
    f(outputStream) // throws
    outputStream.close()
    write(outputStream.buffer)
  }

  /** Use as a Writer.  Content is replaced with writer contents. */
  def withWriter(f: (Writer => Unit)) {
    withOutputStream { outputStream =>
      val writer = new OutputStreamWriter(outputStream)
      f(writer)
      writer.close()
      // withOutputStream will write()
    }
  }

  /** Clear content (set to ""). */
  def clearContent() {
    setContent(ChannelBuffers.EMPTY_BUFFER)
  }
}


object Message {
  @deprecated("Use MediaType.Json")
  val MediaTypeJson         = "application/json"
  @deprecated("Use MediaType.Javascript")
  val MediaTypeJavascript   = "application/javascript"
  @deprecated("Use MediaType.WwwForm")
  val MediaTypeWwwForm      = "application/x-www-form-urlencoded"
  val CharsetUtf8           = "charset=utf-8"
  val ContentTypeJson       = MediaType.Json + ";" + CharsetUtf8
  val ContentTypeJavascript = MediaType.Javascript + ";" + CharsetUtf8
  val ContentTypeWwwFrom    = MediaType.WwwForm + ";" + CharsetUtf8

  private val HttpDateFormat = FastDateFormat.getInstance("EEE, dd MMM yyyy HH:mm:ss",
                                                          TimeZone.getTimeZone("GMT"))
  def httpDateFormat(date: Date): String =
    HttpDateFormat.format(date) + " GMT"
}
