package com.twitter.finagle.http

import scala.annotation.{switch, tailrec}
import scala.collection.mutable

/**
 * Mutable, non-thread-safe [[HeaderMap]] implementation. Any concurrent access
 * to instances of this class should be synchronized externally.
 */
private final class DefaultHeaderMap extends HeaderMap {

  // In general, HashSet/HashTables that are not thread safe are not
  // durable to concurrent modification and can result in infinite loops.
  // As such, we synchronize on the underlying `Headers` when performing
  // accesses to avoid this. In the common case of no concurrent access,
  // this should be cheap.
  private[this] val underlying = new DefaultHeaderMap.Headers

  // ---- HeaderMap -----

  def getAll(key: String): Seq[String] = underlying.synchronized {
    underlying.getAll(key)
  }

  def add(k: String, v: String): HeaderMap = underlying.synchronized {
    underlying.add(k, v)
    this
  }

  def set(key: String, value: String): HeaderMap = underlying.synchronized {
    underlying.set(key, value)
    this
  }

  // ---- Map/MapLike -----

  def get(key: String): Option[String] = underlying.synchronized {
    underlying.getFirst(key)
  }

  def iterator: Iterator[(String, String)] = underlying.synchronized {
    underlying.flattenIterator
  }

  def +=(kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  def -=(key: String): this.type = underlying.synchronized {
    underlying.removeAll(key)
    this
  }

  override def keySet: Set[String] = underlying.synchronized {
    underlying.values.flatMap(_.names).toSet
  }

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

  private final class Header(val name: String, val value: String, var next: Header = null) {

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

    def names: Seq[String] =
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

  // An internal representation for a `MapHeaderMap` that enables efficient
  //
  // - iteration by gaining access to `entriesIterator` (protected method).
  // - get/add functions by providing custom hashCode and equals methods for a key
  private final class Headers extends mutable.HashMap[String, Header] { self =>

    private def hashChar(c: Char): Int =
      if (c >= 'A' && c <= 'Z') c + 32
      else c

    // Adopted from Netty 3 HttpHeaders.
    override protected def elemHashCode(key: String): Int = {
      var result = 0
      var i = key.length - 1

      while (i >= 0) {
        val c = hashChar(key.charAt(i))
        result = 31 * result + c
        i = i - 1
      }

      result
    }

    // Adopted from Netty 3 HttpHeaders.
    override protected def elemEquals(key1: String, key2: String): Boolean =
      if (key1 eq key2) true
      else if (key1.length != key2.length) false
      else {
        @tailrec
        def loop(i: Int): Boolean =
          if (i == key1.length) true
          else {
            val a = key1.charAt(i)
            val b = key2.charAt(i)

            if (a == b || hashChar(a) == hashChar(b)) loop(i + 1)
            else false
          }

        loop(0)
      }

    def flattenIterator: Iterator[(String, String)] = new Iterator[(String, String)] {
      // To get the following behavior, this method must be called in a thread safe
      // manner and as such, it is only called from within the `DefaultHeaderMap.iterator`
      // method, which synchronizes on this instance.
      //
      // The resulting iterator is not invariant of mutations of the HeaderMap, but
      // shouldn't result in corruption of the HashMap. To do that, we make a copy
      // of the underlying values, so by key, it is immutable. However, adding more
      // values to an existing key is still not thread safe in terms of observability
      // since it modifies the `Header` linked list structure, but that shouldn't
      // result in corruption of this HashMap.
      private[this] val it = {
        val array = new Array[Header](self.size)
        val it = self.entriesIterator
        var i = 0
        while (it.hasNext) {
          array(i) = it.next().value
          i += 1
        }
        array.iterator
      }
      private[this] var current: Header = _

      def hasNext: Boolean =
        it.hasNext || current != null

      def next(): (String, String) = {
        if (current == null) {
          current = it.next()
        }

        val result = (current.name, current.value)
        current = current.next
        result
      }
    }

    def getFirst(key: String): Option[String] =
      get(key) match {
        case Some(h) => Some(h.value)
        case None => None
      }

    def getAll(key: String): Seq[String] =
      get(key) match {
        case Some(hs) => hs.values
        case None => Nil
      }

    def add(key: String, value: String): Unit = {
      val h = new Header(key, value)
      get(key) match {
        case Some(hs) => hs.add(h)
        case None => update(key, h)
      }
    }

    def set(key: String, value: String): Unit = {
      val h = new Header(key, value)
      update(key, h)
    }

    def removeAll(key: String): Unit = {
      remove(key)
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
