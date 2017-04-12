package com.twitter.finagle.http

import com.twitter.io.{Buf, Reader => BufReader, Writer => BufWriter}
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.finagle.http.netty.Bijections
import com.twitter.util.{Closable, Duration, Future}
import java.io._
import java.util.{Iterator => JIterator}
import java.nio.charset.Charset
import java.util.{Date, Locale, TimeZone}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.FastDateFormat
import org.jboss.netty.buffer.{
  ChannelBuffer, ChannelBufferInputStream, ChannelBufferOutputStream, ChannelBuffers}
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpMessage}
import scala.collection.JavaConverters._
import Bijections._

/**
 * Rich Message
 *
 * Base class for Request and Response.  There are both input and output
 * methods, though only one set of methods should be used.
 */
abstract class Message {

  protected def httpMessage: HttpMessage
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

  /**
   * Retrieve the current content of this `Message`.
   *
   * If this message is chunked, the resulting `Buf` will always be empty.
   */
  def content: Buf = ChannelBufferBuf.Owned(getContent())

  /**
   * Set the content of this `Message`.
   *
   * Any existing content is discarded. If this `Message` is set to chunked,
   * an `IllegalStateException` is thrown.
   *
   * @see [[content(Buf)]] for Java users
   */
  @throws[IllegalStateException]
  def content_=(content: Buf): Unit = { setContent(BufChannelBuffer(content)) }

  /**
   * Set the content of this `Message`.
   *
   * Any existing content is discarded. If this `Message` is set to chunked,
   * an `IllegalStateException` is thrown.
   *
   * @see [[content_=(Buf)]] for Scala users
   */
  @throws[IllegalStateException]
  final def content(content: Buf): this.type = {
    this.content = content
    this
  }

  /** Get the HTTP version */
  def version: Version = from(httpMessage.getProtocolVersion())

  /** Set the HTTP version
   *
   * @see [[version(Version)]] for Java users
   */
  def version_=(version: Version): Unit = httpMessage.setProtocolVersion(from(version))

  /** Set the HTTP version
   *
   * * @see [[version_=(Version)]] for Scala users
   */
  final def version(version: Version): this.type = {
    this.version = version
    this
  }

  lazy val headerMap: HeaderMap = new Netty3HeaderMap(httpMessage.headers)

  /**
   * Cookies. In a request, this uses the Cookie headers.
   * In a response, it uses the Set-Cookie headers.
   */
  lazy val cookies: CookieMap = new CookieMap(this)
  // Java users: use the interface below for cookies

  /** Get iterator over Cookies */
  def getCookies(): JIterator[Cookie] = cookies.valuesIterator.asJava

  /** Add a cookie */
  def addCookie(cookie: Cookie): Unit = {
    cookies += cookie
  }

  /** Remove a cookie */
  def removeCookie(name: String): Unit = {
    cookies -= name
  }

  /** Accept header */
  def accept: Seq[String] =
    headerMap.get(Fields.Accept) match {
      case Some(s) => s.split(",").map(_.trim).filter(_.nonEmpty)
      case None    => Seq()
    }
  /** Set Accept header */
  def accept_=(value: String): Unit = headerMap.set(Fields.Accept, value)
  /** Set Accept header with list of values */
  def accept_=(values: Iterable[String]): Unit = accept = values.mkString(", ")

  /** Accept header media types (normalized, no parameters) */
  def acceptMediaTypes: Seq[String] =
    accept.map {
      _.split(";", 2).headOption
         .map(_.trim.toLowerCase) // media types are case-insensitive
         .filter(_.nonEmpty)      // skip blanks
    }.flatten

  /** Allow header */
  def allow: Option[String] = headerMap.get(Fields.Allow)
  /** Set Authorization header */
  def allow_=(value: String): Unit = headerMap.set(Fields.Allow, value)
  /** Set Authorization header */
  def allow_=(values: Iterable[Method]): Unit = {
    allow = values.mkString(",").toUpperCase
  }

  /** Get Authorization header */
  def authorization: Option[String] = headerMap.get(Fields.Authorization)
  /** Set Authorization header */
  def authorization_=(value: String): Unit = headerMap.set(Fields.Authorization, value)

  /** Get Cache-Control header */
  def cacheControl: Option[String] = headerMap.get(Fields.CacheControl)
  /** Set Cache-Control header */
  def cacheControl_=(value: String): Unit = headerMap.set(Fields.CacheControl, value)
  /** Set Cache-Control header with a max-age (and must-revalidate). */
  def cacheControl_=(maxAge: Duration): Unit =
    cacheControl = "max-age=" + maxAge.inSeconds.toString + ", must-revalidate"

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
  def charset_=(value: String): Unit = {
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

  /**
   * Get the value of the Content-Length header.  Use length to get the length of actual content.
   *
   * @see [[contentLengthOrElse(Long): Long]] for Java users.
   */
  def contentLength: Option[Long] =
    headerMap.get(Fields.ContentLength).map(_.toLong)

  /**
   * Get the value of the Content-Length header, or the provided default if it doesn't exist.
   *
   * @see [[contentLength: Option(Long)]] for Scala users.
   */
  final def contentLengthOrElse(default: Long): Long = {
    contentLength match {
      case Some(len) => len
      case None => default
    }
  }
  
  /**
   * Set Content-Length header.  Normally, this is automatically set by the
   * Codec, but this method allows you to override that.
   *
   * @see [[contentLength(Long)]] for Java users.
   */
  def contentLength_=(value: Long): Unit =
    headerMap.set(Fields.ContentLength, value.toString)

  /**
   * Set Content-Length header.  Normally, this is automatically set by the
   * Codec, but this method allows you to override that.
   *
   * @see [[contentLength_=(Long)]] for Scala users.
    */
  final def contentLength(value: Long): this.type = {
    this.contentLength = value
    this
  }

  /** Get Content-Type header */
  def contentType: Option[String] = headerMap.get(Fields.ContentType)
  /** Set Content-Type header */
  def contentType_=(value: String): Unit = headerMap.set(Fields.ContentType, value)
  /** Set Content-Type header by media-type and charset */
  def setContentType(mediaType: String, charset: String = "utf-8"): Unit =
    headerMap.set(Fields.ContentType, mediaType + ";charset=" + charset)
  /** Set Content-Type header to application/json;charset=utf-8 */
  def setContentTypeJson(): Unit = headerMap.set(Fields.ContentType, Message.ContentTypeJson)

  /** Get Date header */
  def date: Option[String] = headerMap.get(Fields.Date)
  /** Set Date header */
  def date_=(value: String): Unit = headerMap.set(Fields.Date, value)
  /** Set Date header by Date */
  def date_=(value: Date): Unit = {
    date = Message.httpDateFormat(value)
  }

  /** Get Expires header */
  def expires: Option[String] = headerMap.get(Fields.Expires)
  /** Set Expires header */
  def expires_=(value: String): Unit = headerMap.set(Fields.Expires, value)
  /** Set Expires header by Date */
  def expires_=(value: Date): Unit = {
    expires = Message.httpDateFormat(value)
  }

  /** Get Host header */
  def host: Option[String] = headerMap.get(Fields.Host)
  /** Set Host header */
  def host_=(value: String): Unit = headerMap.set(Fields.Host, value)

  /** Get Last-Modified header */
  def lastModified: Option[String] = headerMap.get(Fields.LastModified)
  /** Set Last-Modified header */
  def lastModified_=(value: String): Unit = headerMap.set(Fields.LastModified, value)
  /** Set Last-Modified header by Date */
  def lastModified_=(value: Date): Unit = {
    lastModified = Message.httpDateFormat(value)
  }

  /** Get Location header */
  def location: Option[String] = headerMap.get(Fields.Location)
  /** Set Location header */
  def location_=(value: String): Unit = headerMap.set(Fields.Location, value)

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
  def mediaType_=(value: String): Unit = {
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
  def referer: Option[String] = headerMap.get(Fields.Referer)
  /** Set Referer [sic] header */
  def referer_=(value: String): Unit = headerMap.set(Fields.Referer, value)

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

  /** Get User-Agent header */
  def userAgent: Option[String] = headerMap.get(Fields.UserAgent)
  /** Set User-Agent header */
  def userAgent_=(value: String): Unit = headerMap.set(Fields.UserAgent, value)

  /** Get WWW-Authenticate header */
  def wwwAuthenticate: Option[String] = headerMap.get(Fields.WwwAuthenticate)
  /** Set WWW-Authenticate header */
  def wwwAuthenticate_=(value: String): Unit = headerMap.set(Fields.WwwAuthenticate, value)

  /** Get X-Forwarded-For header */
  def xForwardedFor: Option[String] = headerMap.get("X-Forwarded-For")
  /** Set X-Forwarded-For header */
  def xForwardedFor_=(value: String): Unit = headerMap.set("X-Forwarded-For", value)

  /**
   * Check if X-Requested-With contains XMLHttpRequest, usually signalling a
   * request from a JavaScript AJAX libraries.  Some servers treat these
   * requests specially.  For example, an endpoint might render JSON or XML
   * instead HTML if it's an XmlHttpRequest.  (Tip: don't do this - it's gross.)
   */
  def isXmlHttpRequest: Boolean =
    headerMap.get("X-Requested-With").exists { _.toLowerCase.contains("xmlhttprequest") }

  /** Get length of content. */
  def length: Int = getContent.readableBytes

  /** Get length of content. */
  final def getLength(): Int = length

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
  def contentString_=(value: String): Unit = {
    if (value != "")
      setContent(BufChannelBuffer(Buf.Utf8(value)))
    else
      setContent(ChannelBuffers.EMPTY_BUFFER)
  }

  /** Set the content as a string. */
  final def setContentString(value: String): Unit = contentString = value

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

  /**
   * Append string to content.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  def write(string: String): Unit = write(Buf.Utf8(string))

  /**
   * Append a Buf to content.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  def write(buf: Buf): Unit = {
    val channelBuffer = buf match {
      case ChannelBufferBuf(channelBuffer) => channelBuffer
      case _ => BufChannelBuffer(buf)
    }
    setContent(ChannelBuffers.wrappedBuffer(getContent(), channelBuffer))
  }

  /**
   * Append bytes to content.
   *
   * This method makes a defensive copy of the provided byte array. This can
   * be avoided by wrapping the byte array via `Buf.ByteArray.Owned` and
   * using the `write(Buf)` method.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  def write(bytes: Array[Byte]): Unit = write(Buf.ByteArray.Shared(bytes))

  /**
   * Append content via an OutputStream.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  def withOutputStream[T](f: OutputStream => T): T = {
    // Use buffer size of 1024.  Netty default is 256, which seems too small.
    // Netty doubles buffers on resize.
    val outputStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(1024))
    val result = f(outputStream) // throws
    outputStream.close()
    write(ChannelBufferBuf.Owned(outputStream.buffer))
    result
  }

  /**
   * Append content via a Writer.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
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
  def clearContent(): Unit = setContent(ChannelBuffers.EMPTY_BUFFER)

  /** End the response stream. */
  def close(): Future[Unit] = writer.close()

  private[http] def isKeepAlive: Boolean = HttpHeaders.isKeepAlive(httpMessage)

  private[this] def headers(): HttpHeaders =
    httpMessage.headers()

  private[this] def getContent(): ChannelBuffer =
    httpMessage.getContent()

  @throws[IllegalStateException]
  private[this] def setContent(content: ChannelBuffer): Unit = {
    // To preserve netty3 behavior, we only throw an exception if the content is non-empty
    if (isChunked && content.readable())
      throw new IllegalStateException("Cannot set non-empty content on chunked message")
    else httpMessage.setContent(content)
  }

  def isChunked: Boolean = httpMessage.isChunked()

  /**
   * Manipulate the `Message` content mode.
   *
   * If `chunked` is `true`, any existing content will be discarded and further attempts
   * to manipulate the synchronous content will result in an `IllegalStateException`.
   *
   * If `chunked` is `false`, the synchronous content methods will become available
   * and the `Reader`/`Writer` of the message will be ignored by finagle.
   */
  def setChunked(chunked: Boolean): Unit =
    httpMessage.setChunked(chunked)
}


object Message {
  private[http] val Utf8          = Charset.forName("UTF-8")
  val CharsetUtf8           = "charset=utf-8"
  val ContentTypeJson       = MediaType.Json + ";" + CharsetUtf8
  val ContentTypeJsonPatch  = MediaType.JsonPatch + ";" + CharsetUtf8
  val ContentTypeJavascript = MediaType.Javascript + ";" + CharsetUtf8
  val ContentTypeWwwForm    = MediaType.WwwForm + ";" + CharsetUtf8

  @deprecated("Use ContentTypeWwwForm instead", "2017-01-06")
  val ContentTypeWwwFrom = ContentTypeWwwForm

  private val HttpDateFormat = FastDateFormat.getInstance("EEE, dd MMM yyyy HH:mm:ss",
                                                          TimeZone.getTimeZone("GMT"),
                                                          Locale.ENGLISH)
  def httpDateFormat(date: Date): String =
    HttpDateFormat.format(date) + " GMT"
}
