package com.twitter.finagle.memcached.util

/**
 * A specialized mechanism for removing a `Set` of elements
 * from a collection.
 *
 * This can be particularly useful when calculating
 * cache misses given the input keys and the hits.
 *
 * @param cutoff used to decide when to use `Set.contains` probing on
 * the `toRemove` set. This number is best picked empirically using
 * benchmarks. The value supplied via the companion object should
 * suffice for most cache use cases.
 */
private[twitter] class NotFound(val cutoff: Double) {

  assert(cutoff >= 0.0 && cutoff <= 1.0)

  private[this] def buildSet[E](from: Iterable[E], toRemove: Set[E]): Set[E] = {
    val remaining = Set.newBuilder[E]
    val iter = from.iterator
    while (iter.hasNext) {
      val k = iter.next()
      if (!toRemove.contains(k))
        remaining += k
    }
    remaining.result()
  }

  /**
   * Remove all elements of `toRemove` from `from`.
   */
  def apply[E](from: Seq[E], toRemove: Set[E]): Set[E] = {
    if (from.isEmpty) {
      Set.empty
    } else if (toRemove.isEmpty) {
      from.toSet
    } else if (toRemove.size >= (from.size * cutoff)) {
      buildSet(from, toRemove)
    } else {
      from.toSet -- toRemove
    }
  }

  /**
   * Remove all elements of `toRemove` from `from`.
   */
  def apply[E](from: Set[E], toRemove: Set[E]): Set[E] = {
    if (from.isEmpty || toRemove.isEmpty) {
      from
    } else if (toRemove.size >= (from.size * cutoff)) {
      buildSet(from, toRemove)
    } else {
      from -- toRemove
    }
  }

  /**
   * Remove all elements of `toRemove` that are keys in `from`.
   */
  def apply[K, V](from: Map[K, V], toRemove: Set[K]): Map[K, V] = {
    if (from.isEmpty || toRemove.isEmpty) {
      from
    } else if (toRemove.size >= (from.size * cutoff)) {
      val remaining = Map.newBuilder[K, V]
      val iter = from.iterator
      while (iter.hasNext) {
        val kv = iter.next()
        if (!toRemove.contains(kv._1))
          remaining += kv
      }
      remaining.result()
    } else {
      from -- toRemove
    }
  }

}

/**
 * Uses an empirically good cutoff for when to use the probing algorithm.
 */
private[twitter] object NotFound extends NotFound(0.6)
