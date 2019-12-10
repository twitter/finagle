package com.twitter.finagle.http

import com.twitter.finagle.http.headers.Rfc7230HeaderValidation.{
  ObsFoldDetected,
  ValidationFailure,
  ValidationSuccess
}
import com.twitter.finagle.http.headers._
import com.twitter.logging.Logger
import com.twitter.util.TwitterDateFormat
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}
import scala.collection.mutable

/**
 * Mutable message headers map.
 *
 * Header names are case-insensitive.  For example, `get("accept")` is the same as
 * get("Accept").
 *
 * The map is a multi-map.  Use [[getAll]] to get all values for a key.  Use [[add]]
 * to append a key-value.
 */
abstract class HeaderMap extends HeaderMapVersionSpecific with mutable.Map[String, String] {

  /**
   * Retrieves all values for a given header name.
   */
  def getAll(key: String): Seq[String]

  /**
   * Retrieves the given header value or `null` if it doesn't exit.
   */
  def getOrNull(key: String): String = get(key).orNull

  /**
   * Retrieves the given header value wrapped with `Option`.
   */
  def get(key: String): Option[String]

  /**
   * Adds a header but don't replace existing header(s).
   */
  def add(k: String, v: String): HeaderMap

  /**
   * Adds a header without replacing existing headers without validating the
   * key and value.
   */
  def addUnsafe(k: String, v: String): HeaderMap

  /**
   * Adds a header without replacing existing headers, as in [[add(String, String)]],
   * but with standard formatting for dates in HTTP headers.
   */
  def add(k: String, date: Date): HeaderMap =
    add(k, HeaderMap.format(date))

  /**
   * Set a header. If an entry already exists, it is replaced.
   */
  def set(k: String, v: String): this.type

  /**
   * Set or replace a header without validating the key and value.
   */
  def setUnsafe(k: String, v: String): this.type

  /**
   * Set or replace a header, as in [[set(String, String)]],
   * but with standard formatting for dates in HTTP headers.
   */
  def set(k: String, date: Date): HeaderMap =
    set(k, HeaderMap.format(date))

  /**
   * Set or replace a header, as in [[+=((String, String))]],
   * but with standard formatting for dates in HTTP headers.
   */
  @deprecated("Use `.set(String, Date)` instead", "2017-02-01")
  def +=(kv: (String, Date)): HeaderMap =
    +=((kv._1, HeaderMap.format(kv._2)))

  override def empty: HeaderMap = DefaultHeaderMap()

  private[finagle] def nameValueIterator: Iterator[HeaderMap.NameValue] =
    iterator.map { case (n, v) => new HeaderMap.NameValueImpl(n, v) }

}

object HeaderMap {
  private[this] val logger = Logger.get(classOf[HeaderMap])

  /**
   * Empty, read-only [[HeaderMap]].
   */
  val Empty: HeaderMap = new EmptyHeaderMap

  /** Create a new HeaderMap from header list.
   *
   * @note the headers are added to the new `HeaderMap` via `add` operations.
   */
  def apply(headers: (String, String)*): HeaderMap =
    DefaultHeaderMap(headers: _*)

  /** Create a new, empty HeaderMap. */
  def newHeaderMap: HeaderMap = apply()

  def hashChar(c: Char): Int =
    if (c >= 'A' && c <= 'Z') c + 32
    else c

  private[this] val formatter = new ThreadLocal[SimpleDateFormat] {
    override protected def initialValue(): SimpleDateFormat = {
      val f = TwitterDateFormat("E, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH)
      f.setTimeZone(TimeZone.getTimeZone("GMT"))
      f
    }
  }

  private def format(date: Date): String =
    if (date == null) null
    else formatter.get().format(date)

  // Exposed for testing
  private[http] val ObsFoldRegex = "\r?\n[\t ]+".r

  private[http] def validateName(name: String): Unit =
    Rfc7230HeaderValidation.validateName(name) match {
      case ValidationSuccess => () // nop
      case ValidationFailure(ex) => throw ex
    }

  private[http] def foldReplacingValidateValue(name: String, value: String): String =
    Rfc7230HeaderValidation.validateValue(name, value) match {
      case ValidationSuccess =>
        value
      case ValidationFailure(ex) =>
        throw ex
      case ObsFoldDetected =>
        logger.debug("`obs-fold` sequence replaced.")
        // Per https://tools.ietf.org/html/rfc7230#section-3.2.4, an obs-fold is equivalent
        // to a SP char and suggests that such header values should be 'fixed' before
        // interpreting or forwarding the message.
        Rfc7230HeaderValidation.replaceObsFold(value)
    }

  private[finagle] trait NameValue {
    def name: String
    def value: String
  }

  private final class NameValueImpl(val name: String, val value: String) extends NameValue

}
