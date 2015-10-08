package com.twitter.finagle.http

import com.twitter.io.{Buf, Reader => BufReader, Writer => BufWriter}
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.http.netty.{HttpMessageProxy, Bijections}
import com.twitter.util.{Await, Duration, Closable}
import java.io.{InputStream, InputStreamReader, OutputStream, OutputStreamWriter, Reader, Writer}
import java.util.{Iterator => JIterator}
import java.nio.charset.Charset
import java.util.{Date, TimeZone}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.FastDateFormat
import org.jboss.netty.buffer.{
  ChannelBufferInputStream, DynamicChannelBuffer, ChannelBuffer,
  ChannelBufferOutputStream, ChannelBuffers
}
import scala.collection.JavaConverters._

import Bijections._

/**
 * Rich Message
 *
 * Base class for Request and Response.  There are both input and output
 * methods, though only one set of methods should be used.
 */
abstract class Message extends HttpMessageProxy {

  private[this] val readerWriter = BufReader.writable()

  /**
   * A read-only handle to the internal stream of bytes, representing the
   * message body. See [[com.twitter.io.Reader]] for more information.
   **/
  def reader: BufReader = readerWriter

  /**
   * A write-only handle to the internal stream of bytes, representing the
   * message body. See [[com.twitter.io.Writer]] for more information.
   **/
  def writer: BufWriter with Closable = readerWriter

  def isRequest: Boolean
  def isResponse = !isRequest

  // XXX should we may be using the Shared variants here?
  def content: Buf = ChannelBufferBuf.Owned(getContent())
  def content_=(content: Buf) { setContent(BufChannelBuffer(content)) }

  def version: Version = from(getProtocolVersion())
  def version_=(version: Version) { setProtocolVersion(from(version)) }

  lazy val headerMap: HeaderMap = new MessageHeaderMap(this)

  /**
   * Cookies. In a request, this uses the Cookie headers.
   * In a response, it uses the Set-Cookie headers.
   */
  lazy val cookies = new CookieMap(this)
  // Java users: use the interface below for cookies

  /** Get iterator over Cookies */
  def getCookies(): JIterator[Cookie] = cookies.valuesIterator.asJava

  /** Add a cookie */
  def addCookie(cookie: Cookie) {
    cookies += cookie
  }

  /** Remove a cookie */
  def removeCookie(name: String) {
    cookies -= name
  }

  /** Accept header */
  def accept: Seq[String] =
    Option(headers.get(Fields.Accept)) match {
      case Some(s) => s.split(",").map(_.trim).filter(_.nonEmpty)
      case None    => Seq()
    }
  /** Set Accept header */
  def accept_=(value: String) { headers.set(Fields.Accept, value) }
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
  def allow: Option[String] = Option(headers.get(Fields.Allow))
  /** Set Authorization header */
  def allow_=(value: String) { headers.set(Fields.Allow, value) }
  /** Set Authorization header */
  def allow_=(values: Iterable[Method]) { allow = values.mkString(",").toUpperCase }

  /** Get Authorization header */
  def authorization: Option[String] = Option(headers.get(Fields.Authorization))
  /** Set Authorization header */
  def authorization_=(value: String) { headers.set(Fields.Authorization, value) }

  /** Get Cache-Control header */
  def cacheControl: Option[String] = Option(headers.get(Fields.CacheControl))
  /** Set Cache-Control header */
  def cacheControl_=(value: String) { headers.set(Fields.CacheControl, value) }
  /** Set Cache-Control header with a max-age (and must-revalidate). */
  def cacheControl_=(maxAge: Duration) {
    cacheControl = "max-age=" + maxAge.inSeconds.toString + ", must-revalidate"
  }

  /** Get charset from Content-Type header */
  def charset: Option[String] = {
    contentType.foreach { contentType =>
      val parts = StringUtils.split(contentType, ';')
      1.to(parts.length - 1) foreach { i =>
        val part = parts(i).trim
        if (part.startsWith("charset=")) {
          val equalsIndex = part.indexOf('=')
          val charset = part.substring(equalsIndex + 1)
          return Some(charset)
        }
      }
    }
    None
  }
  /** Set charset in Content-Type header.  This does not change the content. */
  def charset_=(value: String) {
    val contentType = this.contentType.getOrElse("")
    val parts = StringUtils.split(contentType, ';')
    if (parts.isEmpty) {
      this.contentType = ";charset=" + value // malformed
      return
    }

    val builder = new StringBuilder(parts(0))
    if (!(parts.exists { _.trim.startsWith("charset=") })) {
      // No charset parameter exist, add charset after media type
      builder.append(";charset=")
      builder.append(value)
      // Copy other parameters
      1.to(parts.length - 1) foreach {  i =>
        builder.append(";")
        builder.append(parts(i))
      }
    } else {
      // Replace charset= parameter(s)
      1.to(parts.length - 1) foreach {  i =>
        val part = parts(i)
        if (part.trim.startsWith("charset=")) {
          builder.append(";charset=")
          builder.append(value)
        } else {
          builder.append(";")
          builder.append(part)
        }
      }
    }
    this.contentType = builder.toString
  }

  /** Get Content-Length header.  Use length to get the length of actual content. */
  def contentLength: Option[Long] =
    Option(headers.get(Fields.ContentLength)).map { _.toLong }
  /** Set Content-Length header.  Normally, this is automatically set by the
    * Codec, but this method allows you to override that. */
  def contentLength_=(value: Long) {
    headers.set(Fields.ContentLength, value.toString)
  }

  /** Get Content-Type header */
  def contentType: Option[String] = Option(headers.get(Fields.ContentType))
  /** Set Content-Type header */
  def contentType_=(value: String) { headers.set(Fields.ContentType, value) }
  /** Set Content-Type header by media-type and charset */
  def setContentType(mediaType: String, charset: String = "utf-8") {
    headers.set(Fields.ContentType, mediaType + ";charset=" + charset)
  }
  /** Set Content-Type header to application/json;charset=utf-8 */
  def setContentTypeJson() { headers.set(Fields.ContentType, Message.ContentTypeJson) }

  /** Get Date header */
  def date: Option[String] = Option(headers.get(Fields.Date))
  /** Set Date header */
  def date_=(value: String) { headers.set(Fields.Date, value) }
  /** Set Date header by Date */
  def date_=(value: Date) { date = Message.httpDateFormat(value) }

  /** Get Expires header */
  def expires: Option[String] = Option(headers.get(Fields.Expires))
  /** Set Expires header */
  def expires_=(value: String) { headers.set(Fields.Expires, value) }
  /** Set Expires header by Date */
  def expires_=(value: Date) { expires = Message.httpDateFormat(value) }

  /** Get Host header */
  def host: Option[String] =  Option(headers.get(Fields.Host))
  /** Set Host header */
  def host_=(value: String) { headers.set(Fields.Host, value) }

  /** Get Last-Modified header */
  def lastModified: Option[String] = Option(headers.get(Fields.LastModified))
  /** Set Last-Modified header */
  def lastModified_=(value: String) { headers.set(Fields.LastModified, value) }
  /** Set Last-Modified header by Date */
  def lastModified_=(value: Date) { lastModified = Message.httpDateFormat(value) }

  /** Get Location header */
  def location: Option[String] = Option(headers.get(Fields.Location))
  /** Set Location header */
  def location_=(value: String) { headers.set(Fields.Location, value) }

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
  /**
   * Set media-type in Content-Type header.  Charset and parameter values are
   * preserved, though may not be appropriate for the new media type.
   */
  def mediaType_=(value: String) {
    contentType match {
      case Some(contentType) =>
        val parts = StringUtils.split(contentType, ";", 2)
        if (parts.length == 2) {
          this.contentType = value + ";" + parts(1)
        } else {
          this.contentType = value
        }
      case None =>
        this.contentType = value
    }
  }

  /** Get Referer [sic] header */
  def referer: Option[String] = Option(headers.get(Fields.Referer))
  /** Set Referer [sic] header */
  def referer_=(value: String) { headers.set(Fields.Referer, value) }

  /** Get Retry-After header */
  def retryAfter: Option[String] = Option(headers.get(Fields.RetryAfter))
  /** Set Retry-After header */
  def retryAfter_=(value: String) { headers.set(Fields.RetryAfter, value) }
  /** Set Retry-After header by seconds */
  def retryAfter_=(value: Long) { retryAfter = value.toString }

  /** Get Server header */
  def server: Option[String] = Option(headers.get(Fields.Server))
  /** Set Server header */
  def server_=(value: String) { headers.set(Fields.Server, value) }

  /** Get User-Agent header */
  def userAgent: Option[String] = Option(headers.get(Fields.UserAgent))
  /** Set User-Agent header */
  def userAgent_=(value: String) { headers.set(Fields.UserAgent, value) }

  /** Get WWW-Authenticate header */
  def wwwAuthenticate: Option[String] = Option(headers.get(Fields.WwwAuthenticate))
  /** Set WWW-Authenticate header */
  def wwwAuthenticate_=(value: String) { headers.set(Fields.WwwAuthenticate, value) }

  /** Get X-Forwarded-For header */
  def xForwardedFor: Option[String] = Option(headers.get("X-Forwarded-For"))
  /** Set X-Forwarded-For header */
  def xForwardedFor_=(value: String) { headers.set("X-Forwarded-For", value) }

  /**
   * Check if X-Requested-With contains XMLHttpRequest, usually signalling a
   * request from a JavaScript AJAX libraries.  Some servers treat these
   * requests specially.  For example, an endpoint might render JSON or XML
   * instead HTML if it's an XmlHttpRequest.  (Tip: don't do this - it's gross.)
   */
  def isXmlHttpRequest = {
    Option(headers.get("X-Requested-With")) exists { _.toLowerCase.contains("xmlhttprequest") }
  }

  /** Get length of content. */
  def length: Int = getContent.readableBytes
  def getLength(): Int = length

  /** Get the content as a string. */
  def contentString: String = {
    val encoding = try {
      Charset.forName(charset getOrElse "UTF-8")
    } catch {
      case _: Throwable => Message.Utf8
    }
    getContent.toString(encoding)
  }

  def getContentString(): String = contentString

  /** Set the content as a string. */
  def contentString_=(value: String) {
    if (value != "")
      setContent(BufChannelBuffer(Buf.Utf8(value)))
    else
      setContent(ChannelBuffers.EMPTY_BUFFER)
  }
  def setContentString(value: String) { contentString = value }

  /**
   * Use content as InputStream.  The underlying channel buffer's reader
   * index is advanced.  (Scala interface.  Java users can use getInputStream().)
   */
  def withInputStream[T](f: InputStream => T): T = {
    val inputStream = getInputStream()
    val result = f(inputStream) // throws
    inputStream.close()
    result
  }

  /**
   * Get InputStream for content.  Caller must close.  (Java interface.  Scala
   * users should use withInputStream.)
   */
  def getInputStream(): InputStream =
    new ChannelBufferInputStream(getContent)

  /** Use content as Reader.  (Scala interface.  Java usrs can use getReader().) */
  def withReader[T](f: Reader => T): T = {
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

  /** Append ChannelBuffer to content.
   *
   * If `isChunked` then multiple writes must be composed using `writer` and
   * `flatMap` to have the appropriate backpressure semantics.
   *
   * Attempting to `write` after calling `close` will result in a thrown
   * [[com.twitter.io.Reader.ReaderDiscarded]].
   */
  @throws(classOf[BufReader.ReaderDiscarded])
  @throws(classOf[IllegalStateException])
  def write(buffer: ChannelBuffer) {
    if (isChunked) writeChunk(buffer) else {
      getContent match {
        case ChannelBuffers.EMPTY_BUFFER =>
          setContent(buffer)
        case content =>
          setContent(ChannelBuffers.wrappedBuffer(content, buffer))
      }
    }
  }

  /**
   * Use content as OutputStream.  Content is replaced with stream contents.
   * (Java users can use this with a Function, or use Netty's ChannelBufferOutputStream
   * and then call setContent() with the underlying buffer.)
   */
  def withOutputStream[T](f: OutputStream => T): T = {
    // Use buffer size of 1024.  Netty default is 256, which seems too small.
    // Netty doubles buffers on resize.
    val outputStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(1024))
    val result = f(outputStream) // throws
    outputStream.close()
    write(outputStream.buffer)
    result
  }

  /** Use as a Writer.  Content is replaced with writer contents. */
  def withWriter[T](f: Writer => T): T = {
    withOutputStream { outputStream =>
      val writer = new OutputStreamWriter(outputStream, Message.Utf8)
      val result = f(writer)
      writer.close()
      // withOutputStream will write()
      result
    }
  }

  /** Clear content (set to ""). */
  def clearContent() {
    setContent(ChannelBuffers.EMPTY_BUFFER)
  }

  /** End the response stream. */
  def close() = writer.close()

  private[this] def writeChunk(buf: ChannelBuffer) {
    if (buf.readable) {
      val future = writer.write(new ChannelBufferBuf(buf))
      // Unwraps the future in the Return case, or throws exception in the Throw case.
      if (future.isDefined) Await.result(future)
    }
  }
}


object Message {
  private[http] val Utf8          = Charset.forName("UTF-8")
  val CharsetUtf8           = "charset=utf-8"
  val ContentTypeJson       = MediaType.Json + ";" + CharsetUtf8
  val ContentTypeJavascript = MediaType.Javascript + ";" + CharsetUtf8
  val ContentTypeWwwFrom    = MediaType.WwwForm + ";" + CharsetUtf8

  private val HttpDateFormat = FastDateFormat.getInstance("EEE, dd MMM yyyy HH:mm:ss",
                                                          TimeZone.getTimeZone("GMT"))
  def httpDateFormat(date: Date): String =
    HttpDateFormat.format(date) + " GMT"
}
