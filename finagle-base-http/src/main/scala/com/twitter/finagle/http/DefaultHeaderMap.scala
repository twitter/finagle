package com.twitter.finagle.http

import java.util.Locale
import scala.annotation.switch
import scala.collection.mutable

/**
 * Mutable, non-thread-safe [[HeaderMap]] implementation. Any concurrent access
 * to instances of this class should be synchronized externally.
 */
private final class DefaultHeaderMap extends HeaderMap {

  import DefaultHeaderMap._

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

  def iterator: Iterator[(String, String)] = underlying.flattenIterator

  def +=(kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  def -=(key: String): this.type = {
    underlying.removeAll(key)
    this
  }

  override def keySet: Set[String] =
    underlying.values.flatMap(_.keys).toSet

  override def keysIterator: Iterator[String] =
    keySet.iterator
}

private object DefaultHeaderMap {
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

  private class Header(val name: String, val value: String, var next: Header = null) {
    validateName(name)
    validateValue(value)

    def values: Seq[String] =
      if (next == null) value :: Nil
      else {
        val result = new mutable.ListBuffer[String] += value

        var i = next
        do {
          result += i.value
          i = i.next
        } while (i != null)

        result.toList
      }

    def keys: Seq[String] =
      if (next == null) name :: Nil
      else {
        val result = new mutable.ListBuffer[String] += name

        var i = next
        do {
          result += i.name
          i = i.next
        } while (i != null)

        result.toList
      }

    def add(h: Header): Unit = {
      var i = this
      while (i.next != null) {
        i = i.next
      }

      i.next = h
    }
  }

  // An internal representation for a `MapHeaderMap` that enables efficient iteration
  // by gaining access to `entriesIterator` (protected method).
  private class Headers extends mutable.HashMap[String, Header] {

    @inline private final def canonicalName(s: String): String = s.toLowerCase(Locale.US)

    def flattenIterator: Iterator[(String, String)] = new Iterator[(String, String)] {
      private[this] val it = entriesIterator
      private[this] var current: Header = _

      def hasNext: Boolean =
        it.hasNext || current != null

      def next(): (String, String) = {
        if (current == null) {
          current = it.next().value
        }

        val result = (current.name, current.value)
        current = current.next
        result
      }
    }

    def getFirst(key: String): Option[String] =
      get(canonicalName(key)) match {
        case Some(h) => Some(h.value)
        case None => None
      }

    def getAll(key: String): Seq[String] =
      get(canonicalName(key)) match {
        case Some(hs) => hs.values
        case None => Nil
      }

    def add(key: String, value: String): Unit = {
      val h = new Header(key, value)
      val cn = canonicalName(key)
      get(cn) match {
        case Some(hs) => hs.add(h)
        case None => update(cn, h)
      }
    }

    def set(key: String, value: String): Unit = {
      val h = new Header(key, value)
      update(canonicalName(key), h)
    }

    def removeAll(key: String): Unit = {
      remove(canonicalName(key))
    }
  }

  /** Construct a new `HeaderMap` with the header list
   *
   * @note the headers are added to this `HeaderMap` via an `add` operation.
   */
  def apply(headers: (String, String)*): HeaderMap = {
    val result = new DefaultHeaderMap
    headers.foreach(t => result.add(t._1, t._2))
    result
  }
}
