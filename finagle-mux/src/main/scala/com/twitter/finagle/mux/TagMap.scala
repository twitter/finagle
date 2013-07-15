package com.twitter.finagle.mux

import java.util.HashMap

/**
 * TagMaps maintains a mapping between tags and elements of type `T`.
 * Tags are acquired from- and released to- `set`. TagMap maintains
 * the first `fastSize` tags in an array for efficient access.
 */
private trait TagMap[T] extends Iterable[(Int, T)] {
  def map(el: T): Option[Int]
  def maybeRemap(tag: Int, newEl: T): Option[T]
  def unmap(tag: Int): Option[T]
}

private object TagMap {
  def apply[T <: Object: ClassManifest](
      set: TagSet,
      fastSize: Int = 256
  ): TagMap[T] = new TagMap[T] {
    require(fastSize >= 0)
    private[this] val fast = new Array[T](fastSize)
    private[this] val fallback = new HashMap[Int, T]
    private[this] val fastOff = set.range.start
    private[this] def inFast(tag: Int): Boolean = tag < fastSize+fastOff
    private[this] def getFast(tag: Int): T = fast(tag-fastOff)
    private[this] def setFast(tag: Int, el: T) { fast(tag-fastOff) = el }

    def map(el: T): Option[Int] = synchronized {
      set.acquire() map { tag =>
        if (inFast(tag))
          setFast(tag, el)
        else
          fallback.put(tag, el)
        tag
      }
    }

    def maybeRemap(tag: Int, newEl: T): Option[T] = synchronized {
      if (!contains(tag)) return None

      val oldEl = if (inFast(tag)) {
        val oldEl = getFast(tag)
        setFast(tag, newEl)
        oldEl
      } else {
        val oldEl = fallback.remove(tag)
        fallback.put(tag, newEl)
        oldEl
      }

      Some(oldEl)
    }

    def unmap(tag: Int): Option[T] = synchronized {
      val res = if (inFast(tag)) {
        val el = getFast(tag)
        setFast(tag, null.asInstanceOf[T])
        Option(el)
      } else
        Option(fallback.remove(tag))

      set.release(tag)
      res
    }

    private[this] def contains(tag: Int) =
      (inFast(tag) && getFast(tag) != null) || fallback.containsKey(tag)

    def iterator: Iterator[(Int, T)] = set.iterator flatMap { tag =>
      synchronized {
        val el = if (inFast(tag)) getFast(tag) else fallback.get(tag)
        if (el == null) Iterable.empty
        else Iterator.single((tag, el))
      }
    }
  }
}
