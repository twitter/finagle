package com.twitter.finagle.http

import com.twitter.finagle.http.netty.HttpMessageProxy
import com.twitter.util.TwitterDateFormat
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}
import scala.annotation.varargs
import scala.collection.mutable
import scala.collection.JavaConverters._

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

  def getAll(key: String): Iterable[String]

  /** Add a header but don't replace existing header(s). */
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
  def += (kv: (String, Date)): HeaderMap =
    += ((kv._1, HeaderMap.format(kv._2)))

  override def empty: HeaderMap = new MapHeaderMap(mutable.Map.empty)
}


/** Mutable-Map-backed [[HeaderMap]] */
class MapHeaderMap(underlying: mutable.Map[String, Seq[String]]) extends HeaderMap {

  def getAll(key: String): Iterable[String] =
    underlying.getOrElse(key, Nil)

  def add(k: String, v: String): MapHeaderMap = {
    underlying(k) = underlying.getOrElse(k, Nil) :+ v
    this
  }

  def set(key: String, value: String): MapHeaderMap = {
    underlying.retain { case (a, _) => !a.equalsIgnoreCase(key) }
    underlying(key) = Seq(value)
    this
  }

  // For Map/MapLike
  def get(key: String): Option[String] = {
    underlying.find { case (k, v) => k.equalsIgnoreCase(key) }.flatMap { _._2.headOption }
  }

  // For Map/MapLike
  def iterator: Iterator[(String, String)] = {
    for ((k, vs) <- underlying.iterator; v <- vs) yield
      (k, v)
  }

  // For Map/MapLike
  def += (kv: (String, String)): MapHeaderMap.this.type = {
    underlying(kv._1) = Seq(kv._2)
    this
  }

  // For Map/MapLike
  def -= (key: String): MapHeaderMap.this.type = {
    underlying.retain { case (a, b) => !a.equalsIgnoreCase(key) }
    this
  }

  override def keys: Iterable[String] =
    underlying.keys

  override def keySet: Set[String] =
    underlying.keySet.toSet

  override def keysIterator: Iterator[String] =
    underlying.keysIterator
}


object MapHeaderMap {
  def apply(headers: Tuple2[String, String]*): MapHeaderMap = {
    val map = headers
      .groupBy { case (k, v) => k.toLowerCase }
      .mapValues { case values => values.map { _._2 } } // remove keys
    new MapHeaderMap(mutable.Map() ++ map)
  }
}


/**
 * Mutable HttpMessage-backed [[HeaderMap]].
 */
private[finagle] class MessageHeaderMap(httpMessage: HttpMessageProxy) extends HeaderMap {
  def get(key: String): Option[String] =
    Option(httpMessage.headers.get(key))

  def iterator: Iterator[(String, String)] =
    httpMessage.headers.iterator.asScala.map { entry =>
      (entry.getKey, entry.getValue)
    }

  override def keys: Iterable[String] =
    httpMessage.headers.names.asScala

  override def keySet: Set[String] =
    keys.toSet

  override def keysIterator: Iterator[String] =
    keySet.iterator

  override def contains(key: String): Boolean =
    httpMessage.headers.contains(key)

  def += (kv: (String, String)): MessageHeaderMap.this.type = {
    httpMessage.headers.set(kv._1, kv._2)
    this
  }

  def -= (key: String): MessageHeaderMap.this.type = {
    httpMessage.headers.remove(key)
    this
  }

  def getAll(key: String): Iterable[String] =
    httpMessage.headers.getAll(key).asScala

  def set(k: String, v: String): HeaderMap = {
    httpMessage.headers.set(k,v)
    this
  }

  def add(k: String, v: String): MessageHeaderMap = {
    httpMessage.headers.add(k, v)
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
