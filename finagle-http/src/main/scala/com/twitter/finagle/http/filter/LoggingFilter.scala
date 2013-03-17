package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.filter.{
  LogFormatter => CoreLogFormatter,
  LoggingFilter => CoreLoggingFilter
}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Future, Return, Throw, Time, Stopwatch}
import java.util.TimeZone
import org.apache.commons.lang.time.FastDateFormat


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
      if (i >= 0x20 && i <= 0x7E && i != 0x22 && i != 0x5C) {
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
          case '\b'       => builder.append("\\b")
          case '\n'       => builder.append("\\n")
          case '\r'       => builder.append("\\r")
          case '\t'       => builder.append("\\t")
          case BackslashV => builder.append("\\v")
          case '\\'       => builder.append("\\\\")
          case '"'        => builder.append("\\\"")
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
  /* See http://httpd.apache.org/docs/2.0/logs.html
   *
   * Apache common log format is: "%h %l %u %t \"%r\" %>s %b"
   *   %h: remote host
   *   %l: remote logname
   *   %u: remote user
   *   %t: time request was received
   *   %r: request lime
   *   %s: status
   *   %b: bytes
   *
   * We add:
   *   %D: response time in milliseconds
   *   "%{User-Agent}i": user agent
   */
  val DateFormat = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",
                     TimeZone.getTimeZone("GMT"))
  def format(request: Request, response: Response, responseTime: Duration) = {
    val remoteAddr = request.remoteAddress.getHostAddress

    val contentLength = response.length
    val contentLengthStr = if (contentLength > 0) contentLength.toString else "-"

    val uaStr = request.userAgent.getOrElse("-")

    val builder = new StringBuilder
    builder.append(remoteAddr)
    builder.append(" - - [")
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

  def formatException(request: Request, throwable: Throwable, responseTime: Duration): String = throw new UnsupportedOperationException("Log throwables as empty 500s instead")

  def formattedDate(): String =
    DateFormat.format(Time.now.toDate)
}


/**
 *  Logging filter.
 *
 * Logs all requests according to formatter.
 * Note this may be used upstream of a ValidateRequestFilter, so the URL and
 * parameters may be invalid.
 */
class LoggingFilter[REQUEST <: Request](
  val log: Logger,
  val formatter: CoreLogFormatter[REQUEST, Response]
) extends CoreLoggingFilter[REQUEST, Response] {

  // Treat exceptions as empty 500 errors
  override protected def logException(duration: Duration, request: REQUEST, throwable: Throwable) {
    val response = Response(request.version, Status.InternalServerError)
    val line = formatter.format(request, response, duration)
    log.info(line)
  }
}


object LoggingFilter extends LoggingFilter[Request]({
    val log = Logger("access")
    log.setUseParentHandlers(false)
    log
  },
  new CommonLogFormatter)
