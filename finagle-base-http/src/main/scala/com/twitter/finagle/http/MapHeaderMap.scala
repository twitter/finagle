package com.twitter.finagle.http

import java.util.Locale
import scala.annotation.switch
import scala.collection.mutable

/** Mutable-Map-backed [[HeaderMap]] */
private final class MapHeaderMap extends HeaderMap {

  import MapHeaderMap._

  private[this] val underlying = mutable.Map.empty[String, Vector[HeaderValuePair]]

  def getAll(key: String): Seq[String] =
    underlying.getOrElse(canonicalName(key), Vector.empty).map(_.value)

  def add(k: String, v: String): HeaderMap = {
    val t = new HeaderValuePair(k, v)
    underlying(t.canonicalName) = underlying.getOrElse(t.canonicalName, Vector.empty) :+ t
    this
  }

  def set(key: String, value: String): HeaderMap = {
    val t = new HeaderValuePair(key, value)
    underlying(t.canonicalName) = Vector(t)
    this
  }

  // For Map/MapLike
  def get(key: String): Option[String] = getAll(key).headOption

  // For Map/MapLike
  def iterator: Iterator[(String, String)] = {
    for ((_, vs) <- underlying.iterator; v <- vs) yield (v.name, v.value)
  }

  // For Map/MapLike
  def +=(kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  // For Map/MapLike
  def -=(key: String): this.type = {
    underlying.remove(canonicalName(key))
    this
  }

  override def keys: Iterable[String] =
    keySet

  override def keySet: Set[String] =
    underlying.values.flatten.map(_.name).toSet

  override def keysIterator: Iterator[String] =
    keySet.iterator
}

private object MapHeaderMap {

  private class HeaderValuePair(val name: String, val value: String) {
    validateName(name)
    validateValue(value)

    val canonicalName: String = MapHeaderMap.canonicalName(name)
  }

  private def canonicalName(s: String): String = s.toLowerCase(Locale.US)

  // Adopted from Netty 3 HttpHeaders.
  private def validateName(s: String): Unit = {
    if (s == null) throw new NullPointerException("Header names cannot be null")

    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)

      if (c > 127) {
        throw new IllegalArgumentException(
          "Header name cannot contain non-ASCII characters: " + c)
      }

      (c: @switch) match {
        case '\t' | '\n' | 0x0b | '\f' | '\r' | ' ' | ',' | ':' | ';' | '=' =>
          throw new IllegalArgumentException(
            "Header name cannot contain the following prohibited characters: " +
              "=,;: \\t\\r\\n\\v\\f ")
        case _ =>
      }

      i += 1
    }
  }

  // Adopted from Netty 3 HttpHeaders.
  private def validateValue(s: String): Unit = {
    if (s == null) throw new NullPointerException("Header values cannot be null")

    var i = 0

    // 0: Previous character was neither CR nor LF
    // 1: The previous character was CR
    // 2: The previous character was LF
    var state = 0

    while (i < s.length) {
      val c = s.charAt(i)

      (c: @switch) match {
        case 0x0b =>
          throw new IllegalArgumentException(
            "Header value contains a prohibited character '\\v': " + s)
        case '\f' =>
          throw new IllegalArgumentException(
            "Header value contains a prohibited character '\\f': " + s)
        case _ =>
      }

      (state: @switch) match {
        case 0 =>
          if (c == '\r') state = 1
          else if (c == '\n') state = 2
        case 1 =>
          if (c == '\n') state = 2
          else throw new IllegalArgumentException("Only '\\n' is allowed after '\\r': " + s)
        case 2 =>
          if (c == '\t' || c == ' ') state = 0
          else throw new IllegalArgumentException("Only ' ' and '\\t' are allowed after '\\n': " + s)
      }

      i += 1
    }

    if (state != 0) {
      throw new IllegalArgumentException("Header value must not end with '\\r' or '\\n':" + s)
    }
  }

  /** Construct a new `HeaderMap` with the header list
   *
   * @note the headers are added to this `HeaderMap` via an `add` operation.
   */
  def apply(headers: (String, String)*): HeaderMap = {
    val result = new MapHeaderMap
    headers.foreach(t => result.add(t._1, t._2))
    result
  }
}
