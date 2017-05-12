package com.twitter.finagle.http

import com.twitter.finagle.http.MapHeaderMap.HeaderValuePair
import java.util.Locale
import scala.collection.mutable

/** Mutable-Map-backed [[HeaderMap]] */
private final class MapHeaderMap extends HeaderMap {

  private[this] val underlying = mutable.Map.empty[String, Vector[HeaderValuePair]]

  def getAll(key: String): Seq[String] =
    underlying.getOrElse(HeaderValuePair.canonicalName(key), Vector.empty).map(_.value)

  def add(k: String, v: String): HeaderMap = {
    val t = HeaderValuePair(k, v)
    underlying(t.canonicalName) = underlying.getOrElse(t.canonicalName, Vector.empty) :+ t
    this
  }

  def set(key: String, value: String): HeaderMap = {
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
  def +=(kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  // For Map/MapLike
  def -= (key: String): this.type = {
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

private object MapHeaderMap {
  private case class HeaderValuePair(header: String, value: String) {
    val canonicalName = HeaderValuePair.canonicalName(header)
  }

  private object HeaderValuePair {
    def canonicalName(header: String) = header.toLowerCase(Locale.US)
  }

  /** Construct a new `HeaderMap` with the header list
   *
   * @note the headers are added to this `HeaderMap` via an `add` operation.
   */
  def apply(headers: (String, String)*): HeaderMap = {
    val tmp = new MapHeaderMap
    headers.foreach(t => tmp.add(t._1, t._2))
    tmp
  }
}