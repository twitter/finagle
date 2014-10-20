package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.HttpMessage
import scala.collection.mutable
import scala.collection.JavaConverters._


/**
 * Message headers map.
 *
 * Header names are case-insensitive.  For example, get("accept") is the same as
 * get("Accept").
 *
 * The map is a multi-map.  Use getAll() to get all values for a key.  Use add()
 * to append a key-value.
 */
abstract class HeaderMap
  extends mutable.Map[String, String]
  with mutable.MapLike[String, String, HeaderMap] {

  def getAll(key: String): Iterable[String]

  /** Add a header but don't replace existing header(s). */
  def add(k: String, v: String): Unit

  override def empty: HeaderMap = new MapHeaderMap(mutable.Map.empty)
}


/** Mutable-Map-backed HeaderMap */
class MapHeaderMap(underlying: mutable.Map[String, Seq[String]]) extends HeaderMap {

  def getAll(key: String): Iterable[String] =
    underlying.getOrElse(key, Nil)

  def add(k: String, v: String): Unit = {
    underlying(k) = underlying.getOrElse(k, Nil) :+ v
  }

  // For Map/MapLike
  def get(key: String): Option[String] = {
    underlying.find { case (k, v) => k.equalsIgnoreCase(key) } flatMap { _._2.headOption }
  }

  // For Map/MapLike
  def iterator: Iterator[(String, String)] = {
    for ((k, vs) <- underlying.iterator; v <- vs) yield
      (k, v)
  }

  // For Map/MapLike
  def += (kv: (String, String)) = {
    underlying(kv._1) = Seq(kv._2)
    this
  }

  // For Map/MapLike
  def -= (key: String) = {
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
 * HttpMessage-backed HeaderMap.
 */
class MessageHeaderMap(httpMessage: HttpMessage) extends HeaderMap {

  def get(key: String): Option[String] =
    Option(httpMessage.headers.get(key))

  def iterator: Iterator[(String, String)] =
    httpMessage.headers.iterator.asScala map { entry =>
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

  def += (kv: (String, String)) = {
    httpMessage.headers.set(kv._1, kv._2)
    this
  }

  def -= (key: String) = {
    httpMessage.headers.remove(key)
    this
  }

  def getAll(key: String): Iterable[String] =
    httpMessage.headers.getAll(key).asScala

  def add(k: String, v: String) =
    httpMessage.headers.add(k, v)
}


object HeaderMap {
  /** Create HeaderMap from header list.  Convenience method for testing. */
  def apply(headers: Tuple2[String, String]*): HeaderMap =
    MapHeaderMap(headers: _*)
}
