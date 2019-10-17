package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap
import scala.annotation.tailrec
import java.util.function.BiConsumer
import scala.collection.AbstractIterator

/**
 * Mutable, thread-safe [[HeaderMap]] implementation, backed by
 * a mutable [[Map[String, Header]]], where the map key
 * is forced to lower case
 */
final private class JTreeMapBackedHeaderMap extends HeaderMap {
  import HeaderMap._

  // In general, Map's that are not thread safe are not
  // durable to concurrent modification and can result in infinite loops
  // and exceptions.
  // As such, we synchronize on the underlying collection when performing
  // accesses to avoid this. In the common case of no concurrent access,
  // this should be cheap.
  private[this] val underlying: java.util.TreeMap[String, Header.Root] = 
    new java.util.TreeMap[String, Header.Root](JTreeMapBackedHeaderMap.sharedComparator)

  private def foreachConsumer[U](f: ((String, String)) => U):
    BiConsumer[String, Header.Root] = new BiConsumer[String, Header.Root](){
      def accept(key: String, header: Header.Root): Unit = header.iterator.foreach(
        nv => f(nv.name, nv.value)
      )
    }

  override def foreach[U](f: ((String, String)) => U): Unit = 
    underlying.forEach(foreachConsumer(f))

  // ---- HeaderMap -----

  // Validates key and value.
  def add(key: String, value: String): this.type = {
    validateName(key)
    addUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // Validates key and value.
  def set(key: String, value: String): this.type = {
    validateName(key)
    setUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // ---- Map/MapLike -----

  def -=(key: String): this.type = removed(key)
  def +=(kv: (String, String)): this.type = set(kv._1, kv._2)

  /**
   * Underlying headers eagerly copied to an array, without synchronizing
   * on the underlying collection. That means the calling method
   * must synchronize.
   */
  private[this] def copyHeaders: Iterator[Header.Root] =
    underlying.values.toArray(new Array[Header.Root](underlying.size)).iterator

  def iterator: Iterator[(String, String)] =
    nameValueIterator.map(nv => (nv.name, nv.value))

  override def keys: Set[String] = keysIterator.toSet

  override def keysIterator: Iterator[String] = underlying.synchronized {
    Header.uniqueNames(copyHeaders)
  }

  private[finagle] final override def nameValueIterator: Iterator[HeaderMap.NameValue] =
    underlying.synchronized {
      copyHeaders.flatMap(_.iterator)
    }

  def getAll(key: String): Seq[String] = underlying.synchronized {
    underlying.get(key) match {
      case null => Nil
      case r: Header.Root => r.values
    }
  }

  // Does not validate key and value.
  def addUnsafe(key: String, value: String): this.type  = underlying.synchronized {
    underlying.get(key) match {
      case null => underlying.put(key, Header.root(key, value))
      case h    => h.add(key, value)
    }
    this
  }

  // Does not validate key and value.
  def setUnsafe(key: String, value: String): this.type = underlying.synchronized {
    underlying.put(key, Header.root(key, value))
    this
  }

  def get(key: String): Option[String] = underlying.synchronized {
    underlying.get(key) match {
      case null => None
      case h => Some(h.value)
    }
  }

  def removed(key: String): this.type = underlying.synchronized {
    underlying.remove(key)
    this
  }
}


object JTreeMapBackedHeaderMap {

  val sharedComparator = new java.util.Comparator[String] {
    def compare(key1: String, key2: String): Int = {
      // Shorter strings are always less, regardless of their content
      val lenthDiff = key1.length - key2.length
      if (lenthDiff != 0) lenthDiff
      else {
        @tailrec
        def go(i: Int): Int = {
          if (i == key1.length) 0 // end, they are equal.
          else {
            val char1 = HeadersHash.hashChar(key1.charAt(i))
            val char2 = HeadersHash.hashChar(key2.charAt(i))
            val diff = char1 - char2
            if (diff == 0) go(i + 1)
            else diff
          }
        }
        go(0)
      }
    }
  }

  def apply(headers: (String, String)*): HeaderMap = {
    val result = new JTreeMapBackedHeaderMap
    headers.foreach(t => result.add(t._1, t._2))
    result
  }
}
