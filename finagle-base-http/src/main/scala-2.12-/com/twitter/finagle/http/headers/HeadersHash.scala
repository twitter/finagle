package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap
import scala.annotation.tailrec
import scala.collection.mutable

// An internal representation for a `MapHeaderMap` that enables efficient
//
// - iteration by gaining access to `entriesIterator` (protected method).
// - get/add functions by providing custom hashCode and equals methods for a key
private[http] final class HeadersHash extends mutable.HashMap[String, Header.Root] {
  import HeaderMap.hashChar

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

  // This method must be called in a thread safe manner and as such, it is only called from within
  // the `DefaultHeaderMap.*iterator` methods that synchronize on this instance.
  //
  // The resulting iterator is not invariant of mutations of the HeaderMap, but
  // shouldn't result in corruption of the HashMap. To do that, we make a copy
  // of the underlying values, so by key, it is immutable. However, adding more
  // values to an existing key is still not thread safe in terms of observability
  // since it modifies the `Header` linked list structure, but that shouldn't
  // result in corruption of this HashMap.
  private[this] def copiedEntitiesIterator: Iterator[Header] = {
    val array = new Array[Header](size)
    val it = entriesIterator
    var i = 0
    while (it.hasNext) {
      array(i) = it.next().value
      i += 1
    }

    array.iterator
  }

  def uniqueNamesIterator: Iterator[String] =
    Header.uniqueNames(copiedEntitiesIterator)

  def flattenIterator: Iterator[(String, String)] =
    flattenedNameValueIterator.map(nv => (nv.name, nv.value))

  def flattenedNameValueIterator: Iterator[HeaderMap.NameValue] =
    new Iterator[HeaderMap.NameValue] {
      private[this] val it = copiedEntitiesIterator
      private[this] var current: Header = _

      def hasNext: Boolean =
        it.hasNext || current != null

      def next(): HeaderMap.NameValue = {
        if (current == null) {
          current = it.next()
        }

        val result = current
        current = current.next
        result
      }
    }

  def getFirstOrNull(key: String): String =
    findEntry(key) match {
      case null => null
      case e => e.value.value
    }

  def getFirst(key: String): Option[String] =
    Option(getFirstOrNull(key))

  def getAll(key: String): Seq[String] =
    findEntry(key) match {
      case null => Nil
      case e => e.value.values
    }

  def add(key: String, value: String): Unit = {
    findEntry(key) match {
      case null => update(key, Header.root(key, value))
      case e => e.value.add(key, value)
    }
  }

  def set(key: String, value: String): Unit = {
    val h = Header.root(key, value)
    update(key, h)
  }

  def removeAll(key: String): Unit = remove(key)

}
