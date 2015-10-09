package com.twitter.finagle.mux

import java.util.HashMap
import scala.reflect.ClassTag

/**
 * TagMaps maintains a mapping between tags and elements of type `T`.
 * Tags are acquired from- and released to- `set`. TagMap maintains
 * the first `fastSize` tags in an array for efficient access.
 */
private trait TagMap[T] extends Iterable[(Int, T)] {
  /**
   * If a tag is available, an unused tag is returned and `el` is
   * associated with it. Otherwise, None is returned.
   */
  def map(el: T): Option[Int]

  /**
   * If `tag` is currently associated with another element, that
   * element is returned and `tag` is reassociated with
   * `newEl`. Otherwise, None is returned.
   */
  def maybeRemap(tag: Int, newEl: T): Option[T]

  /**
   * If `tag` is currently associated with an element, that element is
   * returned and `tag` the tag is freed. Otherwise, None is returned.
   */
  def unmap(tag: Int): Option[T]

  /**
   * If `tag` is currently associated with an element, that element is
   * returned. Otherwise, None is returned.
   */
  def get(tag: Int): Option[T]
}

private object TagMap {
  def apply[T <: Object: ClassTag](
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

    def get(tag: Int): Option[T] = synchronized {
      if (inFast(tag))
        Option(getFast(tag))
      else
        Option(fallback.get(tag))
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
