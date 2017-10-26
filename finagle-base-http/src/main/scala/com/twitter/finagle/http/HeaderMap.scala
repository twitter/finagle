package com.twitter.finagle.http

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
 *
 * @note This structure isn't thread-safe. Any concurrent access should be synchronized
 *       externally.
 */
abstract class HeaderMap
    extends mutable.Map[String, String]
    with mutable.MapLike[String, String, HeaderMap] {

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
   * Adds a header without replacing existing headers, as in [[add(String, String)]],
   * but with standard formatting for dates in HTTP headers.
   */
  def add(k: String, date: Date): HeaderMap =
    add(k, HeaderMap.format(date))

  /**
   * Set a header. If an entry already exists, it is replaced.
   */
  def set(k: String, v: String): HeaderMap

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
}

object HeaderMap {

  /** Create a new HeaderMap from header list.
   *
   * @note the headers are added to the new `HeaderMap` via `add` operations.
   */
  def apply(headers: (String, String)*): HeaderMap =
    DefaultHeaderMap(headers: _*)

  /** Create a new, empty HeaderMap. */
  def newHeaderMap: HeaderMap = apply()

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
}
