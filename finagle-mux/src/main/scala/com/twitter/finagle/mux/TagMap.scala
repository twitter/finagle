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

    def map(el: T): Option[Int] = synchronized {
      set.acquire() map { tag =>
        if (tag < fastSize+fastOff)
          fast(tag-fastOff) = el
        else
          fallback.put(tag, el)
        tag
      }
    }

    def maybeRemap(tag: Int, newEl: T): Option[T] = synchronized {
      if (!contains(tag)) return None

      val oldEl = if (tag < fastSize+fastOff) {
        val oldEl = fast(tag-fastOff)
        fast(tag-fastOff) = newEl
        oldEl
      } else {
        val oldEl = fallback.remove(tag)
        fallback.put(tag, newEl)
        oldEl
      }

      Some(oldEl)
    }

    def unmap(tag: Int): Option[T] = synchronized {
      val res = if (tag < fastSize+fastOff) {
        val el = fast(tag-fastOff)
        fast(tag-fastOff) = null.asInstanceOf[T]
        Option(el)
      } else
        Option(fallback.remove(tag))

      set.release(tag)
      res
    }

    private[this] def contains(tag: Int) = synchronized {
      (tag < fastSize+fastOff && fast(tag-fastOff) != null) ||
        fallback.containsKey(tag)
    }

    // BUG: weird synchronization semantics
    def iterator: Iterator[(Int, T)] = set.iterator collect {
      case tag if contains(tag) => synchronized {
        val el = if (tag < fastSize+fastOff) fast(tag) else fallback.get(tag)
        (tag, el)
      }
    }
  }
}
