package com.twitter.finagle.http

import com.twitter.util.TwitterDateFormat
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}
import org.jboss.netty.handler.codec.http.HttpHeaders
import scala.annotation.varargs
import scala.collection.JavaConverters._
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
  def += (kv: (String, Date)): HeaderMap =
    += ((kv._1, HeaderMap.format(kv._2)))

  override def empty: HeaderMap = MapHeaderMap()
}

private[finagle] case class HeaderValuePair(header: String, value: String) {
  val canonicalName = HeaderValuePair.canonicalName(header)
}

private[finagle] object HeaderValuePair {
  def canonicalName(header: String) = header.toLowerCase(Locale.US)
}

/** Mutable-Map-backed [[HeaderMap]] */
@deprecated("This is becoming private. Use `HeaderMap.apply(..)` instead.", "2017-02-01")
class MapHeaderMap extends HeaderMap {

  private[this] val underlying = mutable.Map.empty[String, Vector[HeaderValuePair]]

  def getAll(key: String): Seq[String] =
    underlying.getOrElse(HeaderValuePair.canonicalName(key), Vector.empty).map(_.value)

  def add(k: String, v: String): MapHeaderMap = {
    val t = HeaderValuePair(k, v)
    underlying(t.canonicalName) = underlying.getOrElse(t.canonicalName, Vector.empty) :+ t
    this
  }

  def set(key: String, value: String): MapHeaderMap = {
    val t = HeaderValuePair(key, value)
    underlying(t.canonicalName) = Vector(t)
    this
  }

  // For Map/MapLike
  def get(key: String): Option[String] = getAll(key).headOption

  // For Map/MapLike
  def iterator: Iterator[(String, String)] = {
    for ((_, vs) <- underlying.iterator; v <- vs) yield
      (v.header, v.value)
  }

  // For Map/MapLike
  def +=(kv: (String, String)): MapHeaderMap.this.type = {
    set(kv._1, kv._2)
    this
  }

  // For Map/MapLike
  def -= (key: String): MapHeaderMap.this.type = {
    underlying.remove(HeaderValuePair.canonicalName(key))
    this
  }

  override def keys: Iterable[String] =
    keySet

  override def keySet: Set[String] =
    underlying.values.flatten.map(_.header).toSet

  override def keysIterator: Iterator[String] =
    keySet.iterator
}

object MapHeaderMap {
  @deprecated("This is becoming private. Use `HeaderMap.apply(..)` instead.", "2017-02-01")
  def apply(headers: Tuple2[String, String]*): MapHeaderMap = {
    val tmp = new MapHeaderMap
    headers.foreach(t => tmp.add(t._1, t._2))
    tmp
  }
}


/**
 * Mutable HttpMessage-backed [[HeaderMap]].
 */
private final class Netty3HeaderMap(headers: HttpHeaders) extends HeaderMap {

  def get(key: String): Option[String] =
    Option(headers.get(key))

  override def getOrNull(key: String): String =
    headers.get(key)

  def iterator: Iterator[(String, String)] =
    headers.iterator.asScala.map { entry =>
      (entry.getKey, entry.getValue)
    }

  override def keys: Iterable[String] =
    headers.names.asScala

  override def keySet: Set[String] =
    keys.toSet

  override def keysIterator: Iterator[String] =
    keys.iterator

  override def contains(key: String): Boolean =
    headers.contains(key)

  def += (kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  def -= (key: String): this.type = {
    headers.remove(key)
    this
  }

  def getAll(key: String): Seq[String] =
    headers.getAll(key).asScala

  def set(k: String, v: String): HeaderMap = {
    headers.set(k,v)
    this
  }

  def add(k: String, v: String): HeaderMap = {
    headers.add(k, v)
    this
  }
}


object HeaderMap {

  /** Create HeaderMap from header list. */
  @varargs
  def apply(headers: Tuple2[String, String]*): HeaderMap =
    MapHeaderMap(headers: _*)

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
