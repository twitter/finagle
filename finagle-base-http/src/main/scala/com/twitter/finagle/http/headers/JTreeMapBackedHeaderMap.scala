package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap
import scala.annotation.tailrec

/**
 * Mutable, thread-safe [[HeaderMap]] implementation, backed by
 * a mutable [[Map[String, Header]]], where the map key
 * is forced to lower case
 */
final private[http] class JTreeMapBackedHeaderMap extends JMapBackedHeaderMap {
  
  override val underlying: java.util.TreeMap[String, Header] = 
    new java.util.TreeMap[String, Header](JTreeMapBackedHeaderMap.SharedComparitor)

    def getAll(key: String): Seq[String] = underlying.synchronized {
      val got = underlying.get(key.toLowerCase)
      if (got == null) Nil
      else got.values
    }

  // Does not validate key and value.
  def addUnsafe(key: String, value: String): this.type  = underlying.synchronized {
    val header = new Header(key, value)
    underlying.get(key) match {
      case null => underlying.put(key, header)
      case h    => h.add(header)
    }
    this
  }

  // Does not validate key and value.
  def setUnsafe(key: String, value: String): this.type = underlying.synchronized {
    underlying.put(key, new Header(key, value))
    this
  }

  def get(key: String): Option[String] = underlying.synchronized {
    Option(underlying.get(key)).map(_.value)
  }

  def removed(key: String): this.type = underlying.synchronized {
    underlying.remove(key)
    this
  }
}


object JTreeMapBackedHeaderMap {

  val SharedComparitor = new java.util.Comparator[String] {
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

object JHashMapBackedHeaderMap {
  def apply(headers: (String, String)*): HeaderMap = {
    val result = new JHashMapBackedHeaderMap
    headers.foreach(t => result.add(t._1, t._2))
    result
  }
}