package com.twitter.finagle.http.filter

import com.twitter.finagle.filter.{LogFormatter => CoreLogFormatter}
import com.twitter.finagle.filter.{LoggingFilter => CoreLoggingFilter}
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Status
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Logger
import com.twitter.util.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

trait LogFormatter extends CoreLogFormatter[Request, Response] {
  def escape(s: String): String = LogFormatter.escape(s)
}

object LogFormatter {
  private val BackslashV = 0x0b.toByte

  /** Escape string for logging (compatible with Apache's ap_escape_logitem()) */
  def escape(s: String): String = {
    var builder: StringBuilder = null // only create if escaping is needed
    var index = 0
    s.foreach { c =>
      val i = c.toInt
      if (i >= 0x20 && i <= 0x7e && i != 0x22 && i != 0x5c) {
        if (builder == null) {
          index += 1 // common case
        } else {
          builder.append(c)
        }
      } else {
        if (builder == null) {
          builder = new StringBuilder(s.substring(0, index))
        }
        c match {
          case '\b' => builder.append("\\b")
          case '\n' => builder.append("\\n")
          case '\r' => builder.append("\\r")
          case '\t' => builder.append("\\t")
          case BackslashV => builder.append("\\v")
          case '\\' => builder.append("\\\\")
          case '"' => builder.append("\\\"")
          case _ =>
            c.toString().getBytes("UTF-8").foreach { byte =>
              builder.append("\\x")
              val s = java.lang.Integer.toHexString(byte & 0xff)
              if (s.length == 1)
                builder.append("0")
              builder.append(s)
            }
        }
      }
    }
    if (builder == null) {
      s // common case: nothing needed escaping
    } else {
      builder.toString
    }
  }
}

/** Apache-style common log formatter */
class CommonLogFormatter extends LogFormatter {
  /* See https://httpd.apache.org/docs/2.0/logs.html
   *
   * Apache common log format is: "%h %l %u %t \"%r\" %>s %b"
   *   %h: remote host
   *   %l: remote logname
   *   %u: remote user
   *   %t: time request was received
   *   %r: request time
   *   %s: status
   *   %b: bytes
   *
   * We add:
   *   %D: response time in milliseconds
   *   "%{User-Agent}i": user agent
   */
  val DateFormat: DateTimeFormatter =
    DateTimeFormatter
      .ofPattern("dd/MMM/yyyy:HH:mm:ss Z")
      .withLocale(Locale.ENGLISH)
      .withZone(ZoneId.of("GMT"))

  def format(request: Request, response: Response, responseTime: Duration) = {
    val remoteAddr = request.remoteAddress.getHostAddress
    val remoteUser: String = Transport.sslSessionInfo.peerIdentity match {
      case Some(identity) => identity.name
      case _ => "-"
    }

    val contentLength = response.length
    val contentLengthStr = if (contentLength > 0) contentLength.toString else "-"

    val uaStr = request.userAgent.getOrElse("-")

    val builder = new StringBuilder
    builder.append(remoteAddr)
    // %l is unavailable
    builder.append(" - ")
    builder.append(remoteUser)
    builder.append(" [")
    builder.append(formattedDate)
    builder.append("] \"")
    builder.append(escape(request.method.toString))
    builder.append(' ')
    builder.append(escape(request.uri))
    builder.append(' ')
    builder.append(escape(request.version.toString))
    builder.append("\" ")
    builder.append(response.statusCode.toString)
    builder.append(' ')
    builder.append(contentLengthStr)
    builder.append(' ')
    builder.append(responseTime.inMillis)
    builder.append(" \"")
    builder.append(escape(uaStr))
    builder.append('"')

    builder.toString
  }

  def formatException(request: Request, throwable: Throwable, responseTime: Duration): String =
    throw new UnsupportedOperationException("Log throwables as empty 500s instead")

  def formattedDate(): String =
    ZonedDateTime.now.format(DateFormat)
}

/**
 *  Logging filter.
 *
 * Logs all requests according to formatter.
 */
class LoggingFilter[REQUEST <: Request](
  val log: Logger,
  val formatter: CoreLogFormatter[REQUEST, Response])
    extends CoreLoggingFilter[REQUEST, Response] {

  // Treat exceptions as empty 500 errors
  override protected def logException(
    duration: Duration,
    request: REQUEST,
    throwable: Throwable
  ): Unit = {
    val response = Response(request.version, Status.InternalServerError)
    val line = formatter.format(request, response, duration)
    log.info(line)
  }
}

object LoggingFilter
    extends LoggingFilter[Request](
      {
        val log = Logger("access")
        log.setUseParentHandlers(false)
        log
      },
      new CommonLogFormatter)
