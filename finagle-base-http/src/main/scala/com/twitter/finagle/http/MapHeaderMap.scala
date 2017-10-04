package com.twitter.finagle.http

import java.util.Locale
import scala.annotation.switch
import scala.collection.mutable
import scala.collection.JavaConverters._

/** Mutable-Map-backed [[HeaderMap]] */
private final class MapHeaderMap extends HeaderMap {

  import MapHeaderMap._

  private[this] val underlying = new Headers

  // ---- HeaderMap -----

  def getAll(key: String): Seq[String] = underlying.getAll(key)

  def add(k: String, v: String): HeaderMap = {
    underlying.add(k, v)
    this
  }

  def set(key: String, value: String): HeaderMap = {
    underlying.set(key, value)
    this
  }

  // ---- Map/MapLike -----

  def get(key: String): Option[String] = underlying.getFirst(key)

  def iterator: Iterator[(String, String)] = underlying.iterator

  def +=(kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  def -=(key: String): this.type = {
    underlying.remove(canonicalName(key))
    this
  }

  override def keySet: Set[String] =
    underlying.values.asScala.flatten.map(_.name).toSet

  override def keysIterator: Iterator[String] =
    keySet.iterator
}

private object MapHeaderMap {

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

  private class Header(val name: String, val value: String) {
    validateName(name)
    validateValue(value)

    val canonicalName: String = MapHeaderMap.canonicalName(name)
  }

  private val GetHeaderValue: Header => String = _.value
  private val EmptyHeaders = mutable.ArrayBuffer.empty[Header]
  private val NewHeaders = new java.util.function.Function[String, mutable.ArrayBuffer[Header]] {
    def apply(t: String): mutable.ArrayBuffer[Header] = new mutable.ArrayBuffer[Header]
  }

  // An internal representation for a `MapHeaderMap` that enables efficient iteration.
  private class Headers extends java.util.HashMap[String, mutable.ArrayBuffer[Header]] {
    def iterator: Iterator[(String, String)] = new Iterator[(String, String)] {
      private[this] val it = entrySet().iterator()
      private[this] var i = 0
      private[this] var current = EmptyHeaders

      def hasNext: Boolean =
        // We don't expect empty array-buffers in the map.
        it.hasNext || i < current.length

      def next(): (String, String) = {
        if (i == current.length) {
          current = it.next().getValue
          i = 0
        }

        val result = current(i)
        i += 1

        (result.name, result.value)
      }
    }

    def getFirst(key: String): Option[String] = {
      val result = get(canonicalName(key))
      if (result == null) None
      else Some(GetHeaderValue(result(0)))
    }

    def getAll(key: String): Seq[String] = {
      val result = get(canonicalName(key))
      if (result == null) Nil
      else result.map(GetHeaderValue)
    }

    def add(k: String, v: String): Unit = {
      val h = new Header(k, v)
      val hs = computeIfAbsent(h.canonicalName, NewHeaders)

      hs += h
    }

    def set(k: String, v: String): Unit = {
      val h = new Header(k, v)
      val hs = computeIfAbsent(h.canonicalName, NewHeaders)

      hs.clear()
      hs += h
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
