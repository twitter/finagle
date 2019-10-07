package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap
import java.util.function.BiConsumer
import scala.jdk.CollectionConverters._

/**
 * Mutable, thread-safe [[HeaderMap]] implementation, backed by
 * a mutable [[Map[String, Header]]], where the map key
 * is forced to lower case
 */
private[http] trait JMapBackedHeaderMap extends HeaderMap {
  import HeaderMap._

  // In general, HashSet/HashTables that are not thread safe are not
  // durable to concurrent modification and can result in infinite loops.
  // As such, we synchronize on the underlying `Headers` when performing
  // accesses to avoid this. In the common case of no concurrent access,
  // this should be cheap.
  protected val underlying: java.util.Map[String, Header]

  private def foreachConsumer[U](f: ((String, String)) => U):
    BiConsumer[String, Header] = new BiConsumer[String, Header](){
      def accept(key: String, header: Header): Unit = header.iterator.foreach {
        case nv => f(nv.name, nv.value)
      }
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

  final def iterator: Iterator[(String, String)] = underlying.synchronized {
    val underlyingEntries = underlying.entrySet.toArray
    underlyingEntries.toIterator.flatMap {
      case es: java.util.Map.Entry[String @unchecked, Header @unchecked] => {
        es.getValue.iterator.map(nv => (nv.name, nv.value))
      }
    }
  }

  def removed(key: String): this.type

  final override def keys: Set[String] = keysIterator.toSet

  final override def keysIterator: Iterator[String] = underlying.synchronized {
    underlying.values.iterator.asScala.flatMap(header => {
      def rec(uniq: List[String], todo: List[String]): List[String] = todo match {
        case Nil => uniq
        case (head :: tail) =>
          rec(head :: uniq, todo.filterNot(x => x == head))
      }
      rec(Nil, header.names.toList)
    })
  }

  private[finagle] final override def nameValueIterator: Iterator[HeaderMap.NameValue] =
    underlying.synchronized {
      underlying.values.toArray.toIterator.flatMap{ case (h: Header) => h.iterator}
    }
}