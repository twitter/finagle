package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap
import java.util.function.BiConsumer
import scala.collection.AbstractIterator

/**
 * Mutable, thread-safe [[HeaderMap]] implementation, backed by
 * a mutable [[Map[String, Header]]], where the map key
 * is forced to lower case
 */
private[http] trait JMapBackedHeaderMap extends HeaderMap {
  import HeaderMap._

  // In general, Map's that are not thread safe are not
  // durable to concurrent modification and can result in infinite loops
  // and exceptions.
  // As such, we synchronize on the underlying collection when performing
  // accesses to avoid this. In the common case of no concurrent access,
  // this should be cheap.
  protected val underlying: java.util.Map[String, Header.Root]

  private def foreachConsumer[U](f: ((String, String)) => U):
    BiConsumer[String, Header.Root] = new BiConsumer[String, Header.Root](){
      def accept(key: String, header: Header.Root): Unit = header.iterator.foreach(
        nv => f(nv.name, nv.value)
      )
    }

  final override def foreach[U](f: ((String, String)) => U): Unit = 
    underlying.forEach(foreachConsumer(f))

  // ---- HeaderMap -----

  // Validates key and value.
  final def add(key: String, value: String): this.type = {
    validateName(key)
    addUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  def addUnsafe(key: String, value: String): this.type

  // Validates key and value.
  final def set(key: String, value: String): this.type = {
    validateName(key)
    setUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  def setUnsafe(key: String, value: String): this.type

  // ---- Map/MapLike -----

  def -=(key: String): this.type = removed(key)
  def +=(kv: (String, String)): this.type = set(kv._1, kv._2)


  def get(key: String): Option[String]

  /**
   * Underlying headers eagerly copied to an array, without synchronizing
   * on the underlying collection.
   */
  private[this] def copyHeaders: Iterator[Header.Root] = underlying.values.toArray(new Array[Header.Root](underlying.size)).iterator

  final def iterator: Iterator[(String, String)] = underlying.synchronized {
    copyHeaders.flatMap(_.iterator.map(nv => (nv.name, nv.value)))
  }

  def removed(key: String): this.type

  final override def keys: Set[String] = keysIterator.toSet

  final override def keysIterator: Iterator[String] = underlying.synchronized {
    //the common case has a single element in Headers. Prevent unneeded List
    //allocations for that case (don't flatMap)
    val valuesIterator = copyHeaders
    var currentEntries: Iterator[String] = Iterator.empty
    new AbstractIterator[String]{
      def hasNext: Boolean = currentEntries.hasNext || valuesIterator.hasNext
      def next(): String =
        if (currentEntries.hasNext) currentEntries.next()
        else {
          val h = valuesIterator.next()
          if (h.next == null) h.name
          else {
            currentEntries = h.iterator.map(nv => nv.name).toList.distinct.iterator
            currentEntries.next()
          }
        }
    }
  }

  private[finagle] final override def nameValueIterator: Iterator[HeaderMap.NameValue] =
    underlying.synchronized {
      copyHeaders.flatMap(_.iterator)
    }
}