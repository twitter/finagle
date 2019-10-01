package com.twitter.finagle.http.headers

import com.twitter.finagle.http.headers.Rfc7230HeaderValidation.{
  ObsFoldDetected,
  ValidationFailure,
  ValidationSuccess
}
import com.twitter.finagle.http.HeaderMap
import com.twitter.logging.Logger
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Mutable, thread-safe [[HeaderMap]] implementation, backed by
 * a mutable [[Map[String, Header]]], where the map key
 * is forced to lower case
 */
final private[http] class MapBackedHeaderMap extends HeaderMap {
  import HeaderMap._

  // In general, HashSet/HashTables that are not thread safe are not
  // durable to concurrent modification and can result in infinite loops.
  // As such, we synchronize on the underlying `Headers` when performing
  // accesses to avoid this. In the common case of no concurrent access,
  // this should be cheap.
  private[this] val underlying: mutable.Map[String, Header] = mutable.Map.empty

  final override def foreach[U](f: ((String, String)) => U): Unit = for {
    (_, h) <- underlying
    kv <- h.iterator
  } f((h.name, h.value))

  // ---- HeaderMap -----

  final def getAll(key: String): Seq[String] = underlying.synchronized {
    underlying.get(key.toLowerCase).toSeq.flatMap(h => h.values)
  }

  // Validates key and value.
  final def add(key: String, value: String): this.type = {
    validateName(key)
    addUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  final def addUnsafe(key: String, value: String): this.type = underlying.synchronized {
    val lower = key.toLowerCase
    val header = new Header(key, value)
    underlying.get(lower) match {
      case Some(h) => h.add(header)
      case None => underlying.+=((lower, header))
    }
    this
  }

  // Validates key and value.
  final def set(key: String, value: String): this.type = {
    validateName(key)
    setUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  final def setUnsafe(key: String, value: String): this.type = underlying.synchronized {
    underlying.+=((key.toLowerCase, new Header(key, value)))
    this
  }

  // ---- Map/MapLike -----

  def -=(key: String): this.type = removed(key)
  def +=(kv: (String, String)): this.type = set(kv._1, kv._2)


  final def get(key: String): Option[String] = underlying.synchronized {
    underlying.get(key.toLowerCase).map(_.value)
  }

  final def iterator: Iterator[(String, String)] = underlying.synchronized {
    for {
      (_, h) <- underlying.iterator
      hx <- h.iterator
    } yield (hx.name, hx.value)
  }

  final def removed(key: String): this.type = underlying.synchronized {
    underlying.-=(key.toLowerCase)
    this
  }

  final override def keys: Set[String] = keysIterator.toSet

  final override def keysIterator: Iterator[String] = underlying.synchronized {
    underlying.valuesIterator.flatMap(header => {
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
      underlying.valuesIterator.flatMap(_.iterator)
    }
}

object MapBackedHeaderMap {
  def apply(headers: (String, String)*): HeaderMap = {
    val result = new MapBackedHeaderMap
    headers.foreach(t => result.add(t._1, t._2))
    result
  }
}