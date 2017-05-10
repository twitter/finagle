package com.twitter.finagle.memcached.util

import scala.collection.mutable

/**
 * Improve concurrency with fine-grained locking. A hash of synchronized hash
 * tables, keyed on the request key.
 */
class AtomicMap[A, B](maps: Seq[mutable.Map[A, B]]) {
  def this(concurrencyLevel: Int) = this {
    (0 until concurrencyLevel) map { i => mutable.Map[A, B]() }
  }
  def this() = this(16)

  private[this] val concurrencyLevel = maps.size

  def lock[C](key: A)(f: mutable.Map[A, B] => C) = {
    val map = maps((key.hashCode % concurrencyLevel).abs)
    map.synchronized {
      f(map)
    }
  }
}
