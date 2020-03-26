package com.twitter.finagle.http

import com.twitter.finagle.http.Message.BufOutputStream
import com.twitter.finagle.http.util.StringUtil
import com.twitter.io.{Buf, BufInputStream, Reader, Writer}
import com.twitter.util.{Duration, Future}
import java.nio.charset.{Charset, StandardCharsets}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.{Date, Locale, Iterator => JIterator}
import scala.collection.JavaConverters._

/**
 * Rich Message
 *
 * Base class for Request and Response.  There are both input and output
 * methods, though only one set of methods should be used.
 */
abstract class Message {

  @volatile private[this] var _content: Buf = Buf.Empty
  @volatile private[this] var _version: Version = Version.Http11
  @volatile private[this] var _chunked: Boolean = false

  /**
   * A read-only handle to a stream of [[Chunk]], representing the message body. This stream is only
   * populated on chunked messages (`isChunked == true`). Use [[content]] to access a payload of
   * a fully-buffered message (`isChunked == false`).
   *
   * Prefer this API over [[reader]] when application needs to receive trailing headers (trailers).
   * Trailers are transmitted in the very last chunk (`chunk.isLast == true`) of the stream and
   * can be retrieved via [[Chunk.trailers]].
   *
   * @see [[Reader]] and [[Chunk]]
   **/
  def chunkReader: Reader[Chunk]

  /**
   * A write-only handle to a stream of [[Chunk]], representing the message body. Only chunked
   * messages (`isChunked == true`) use this stream as their payload, fully-buffered messages
   * (`isChunked == false`) use [[content]] instead.
   *
   * Prefer this API over [[writer]] when application needs to send trailing headers (trailers).
   * Trailers are transmitted in the very last chunk of the stream and can be populated via
   * `Chunk.last` factory method.
   *
   * @see [[Reader]] and [[Chunk]]
   **/
  def chunkWriter: Writer[Chunk]

  /**
   * A read-only handle to a stream of [[Buf]], representing the message body. This stream is only
   * * populated on chunked messages (`isChunked == true`). Use [[content]] to access a payload of
   * a fully-buffered message (`isChunked == false`).
   *
   * Prefer this API over [[chunkReader]] when application doesn't need access to trailing headers
   * (trailers).
   *
   * @see [[Reader]]
   **/
  final lazy val reader: Reader[Buf] = chunkReader.flatMap(chunk =>
    if (chunk.isLast && chunk.content.isEmpty) Reader.empty
    else Reader.value(chunk.content))

  /**
   * A write-only handle to the stream of [[Buf]], representing the message body. Only chunked
   * messages (`isChunked == true`) use this stream as their payload, fully-buffered messages
   * (`isChunked == false`) use [[content]] instead.
   *
   * Prefer this API over [[chunkWriter]] when application doesn't need to send trailing headers
   * (trailers).
   *
   * @see [[Writer]]
   **/
  final lazy val writer: Writer[Buf] = chunkWriter.contramap(Chunk.apply)

  def isRequest: Boolean
  def isResponse: Boolean = !isRequest

  /**
   * Retrieve the current content of this `Message`.
   *
   * If this message is chunked, the resulting `Buf` will always be empty.
   */
  def content: Buf = _content

  /**
   * Set the content of this `Message`.
   *
   * Any existing content is discarded. If this `Message` is set to chunked,
   * an `IllegalStateException` is thrown.
   *
   * @see [[content(Buf)]] for Java users
   */
  @throws[IllegalStateException]
  def content_=(content: Buf): Unit = {
    if (!content.isEmpty && isChunked)
      throw new IllegalStateException("Cannot set content on Chunked message")
    _content = content
  }

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
  def version: Version = _version

  /** Set the HTTP version
   *
   * @see [[version(Version)]] for Java users
   */
  def version_=(version: Version): Unit = _version = version

  /** Set the HTTP version
   *
   * * @see [[version_=(Version)]] for Scala users
   */
  final def version(version: Version): this.type = {
    this.version = version
    this
  }

  def isChunked: Boolean = _chunked

  /**
   * Manipulate the `Message` content mode.
   *
   * If `chunked` is `true`, any existing content will be discarded and further attempts
   * to manipulate the synchronous content will result in an `IllegalStateException`.
   *
   * If `chunked` is `false`, the synchronous content methods will become available
   * and the `Reader`/`Writer` of the message will be ignored by Finagle.
   */
  def setChunked(chunked: Boolean): Unit = {
    _chunked = chunked
    if (chunked) clearContent()
  }

  /**
   * HTTP headers associated with this message.
   *
   * @note [[HeaderMap]] isn't thread-safe. Any concurrent access should be synchronized
   *       externally.
   */
  def headerMap: HeaderMap

  /**
   * Trailing headers (trailers) associated with this message.
   *
   * These are only populated on fully-buffered inbound messages that were aggregated
   * (see `withStreaming(false)`) from HTTP streams terminating with trailers.
   *
   * @note [[HeaderMap]] isn't thread-safe. Any concurrent access should be synchronized
   *       externally.
   */
  def trailers: HeaderMap

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
    val cType = headerMap.getOrNull(Fields.ContentType)
    if (cType == null)
      return None

    // the format is roughly: "value; charset=value"
    val charsetIdx = cType.indexOf("charset=")
    if (charsetIdx == -1)
      return None

    val valueIdx = charsetIdx + "charset=".length
    val semicolonIdx = cType.indexOf(';', valueIdx)
    val endIdx = if (semicolonIdx == -1) cType.length else semicolonIdx
    Some(cType.substring(valueIdx, endIdx).trim)
  }

  /** Set charset in Content-Type header.  This does not change the content. */
  def charset_=(value: String): Unit = {
    val contentType = this.contentType.getOrElse("")
    val parts = StringUtil.split(contentType, ';')
    if (parts.isEmpty) {
      this.contentType = ";charset=" + value // malformed
      return
    }

    val builder = new StringBuilder(parts(0))
    if (!parts.exists(_.trim.startsWith("charset="))) {
      // No charset parameter exist, add charset after media type
      builder.append(";charset=")
      builder.append(value)
      // Copy other parameters
      1.until(parts.length).foreach { i =>
        builder.append(";")
        builder.append(parts(i))
      }
    } else {
      // Replace charset= parameter(s)
      1.until(parts.length).foreach { i =>
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
    headerMap.setUnsafe(Fields.ContentLength, value.toString)

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
  def setContentTypeJson(): Unit = headerMap.setUnsafe(Fields.ContentType, Message.ContentTypeJson)

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
          case n => contentType.substring(0, n)
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
        val parts = StringUtil.split(contentType, ';', 2)
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
  final def length: Int = content.length

  /** Get length of content. */
  final def getLength(): Int = length

  /** Get the content as a string. */
  def contentString: String = {
    val encoding =
      try {
        Charset.forName(charset.getOrElse("UTF-8"))
      } catch {
        case _: Throwable => StandardCharsets.UTF_8
      }
    Buf.decodeString(content, encoding)
  }

  def getContentString(): String = contentString

  /** Set the content as a string. */
  def contentString_=(value: String): Unit = {
    if (value == "") clearContent()
    else content = Buf.Utf8(value)
  }

  /** Set the content as a string. */
  final def setContentString(value: String): Unit = contentString = value

  /**
   * Use content as InputStream.  The underlying channel buffer's reader
   * index is advanced.  (Scala interface.  Java users can use getInputStream().)
   */
  def withInputStream[T](f: java.io.InputStream => T): T = {
    val inputStream = getInputStream()
    try f(inputStream)
    finally inputStream.close()
  }

  /**
   * Get InputStream for content.  Caller must close.  (Java interface.  Scala
   * users should use withInputStream.)
   */
  final def getInputStream(): java.io.InputStream = new BufInputStream(content)

  /** Use content as Reader.  (Scala interface.  Java users can use getReader().) */
  final def withReader[T](f: java.io.Reader => T): T = {
    withInputStream { inputStream =>
      val reader = new java.io.InputStreamReader(inputStream)
      f(reader)
    }
  }

  /** Get Reader for content.  (Java interface.  Scala users should use withReader.) */
  final def getReader(): java.io.Reader =
    new java.io.InputStreamReader(getInputStream())

  /**
   * Append string to content.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  final def write(string: String): Unit = write(Buf.Utf8(string))

  /**
   * Append a Buf to content.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  final def write(buf: Buf): Unit = {
    if (!isChunked) content = content.concat(buf)
    else throw new IllegalStateException("Cannot write buffers to a chunked message!")
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
  final def write(bytes: Array[Byte]): Unit = write(Buf.ByteArray.Shared(bytes))

  /**
   * Append content via an OutputStream.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  final def withOutputStream[T](f: java.io.OutputStream => T): T = {
    // Use buffer size of 1024 which is only a best guess as to the ideal initial size
    val outputStream = new BufOutputStream(1024)
    try {
      val result = f(outputStream) // throws
      write(outputStream.contentsAsBuf)
      result
    } finally outputStream.close()
  }

  /**
   * Append content via a Writer.
   *
   * An `IllegalStateException` is thrown if this message is chunked.
   */
  @throws(classOf[IllegalStateException])
  final def withWriter[T](f: java.io.Writer => T): T = {
    // withOutputStream will write() to the message
    withOutputStream { outputStream =>
      val writer = new java.io.OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
      try f(writer)
      finally writer.close()
    }
  }

  /** Clear content (set to ""). */
  final def clearContent(): Unit = content = Buf.Empty

  /** End the response stream. */
  def close(): Future[Unit] = writer.close()

  final def keepAlive(keepAlive: Boolean): this.type = {
    version match {
      case Version.Http10 =>
        if (keepAlive) headerMap.setUnsafe(Fields.Connection, "keep-alive")
        else headerMap.remove(Fields.Connection) // HTTP/1.0 defaults to close

      case _ =>
        if (keepAlive) headerMap.remove(Fields.Connection)
        else headerMap.setUnsafe(Fields.Connection, "close")
    }
    this
  }

  final def keepAlive: Boolean = headerMap.get(Fields.Connection) match {
    case Some(value) if value.equalsIgnoreCase("close") => false
    case Some(value) if value.equalsIgnoreCase("keep-alive") => true
    case _ => version == Version.Http11
  }
}

object Message {

  private final class BufOutputStream(initialSize: Int)
      extends java.io.ByteArrayOutputStream(initialSize) {

    def contentsAsBuf: Buf = Buf.ByteArray.Owned(buf, 0, count)
  }

  val CharsetUtf8: String = "charset=utf-8"
  val ContentTypeJson: String = MediaType.Json + ";" + CharsetUtf8
  val ContentTypeJsonPatch: String = MediaType.JsonPatch + ";" + CharsetUtf8
  val ContentTypeJavascript: String = MediaType.Javascript + ";" + CharsetUtf8
  val ContentTypeWwwForm: String = MediaType.WwwForm + ";" + CharsetUtf8

  private val HttpDateFormat: DateTimeFormatter =
    DateTimeFormatter
      .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
      .withLocale(Locale.ENGLISH)
      .withZone(ZoneId.of("GMT"))

  /**
   * Convert a [[java.util.Date]] into an RFC 7231 formatted String representation.
   *
   * @param date the [[java.util.Date]] to format.
   * @return an RFC 7231 formatted String representation of the time represented by the given [[java.util.Date]].
   *
   * @see [[https://tools.ietf.org/html/rfc7231#section-7.1.1.1 RFC 7231 Section 7.1.1.1]]
   */
  def httpDateFormat(date: Date): String = {
    date.toInstant.atOffset(ZoneOffset.UTC).format(HttpDateFormat)
  }

  /**
   * Convert epoch milliseconds into an RFC 7231 formatted String representation.
   *
   * @param millis the milliseconds to format.
   * @return an RFC 7231 formatted String representation of the time represented by the given milliseconds.
   *
   * @see [[https://tools.ietf.org/html/rfc7231#section-7.1.1.1 RFC 7231 Section 7.1.1.1]]
   */
  def httpDateFormat(millis: Long): String = {
    HttpDateFormat.format(Instant.ofEpochMilli(millis))
  }
}
