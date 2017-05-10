package com.twitter.finagle.mux.util

import io.netty.util.collection.IntObjectHashMap

/**
 * TagMaps maintains a mapping between tags and elements of type `T`.
 */
private[mux] trait TagMap[T] {
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

private[mux] object TagMap {
  def apply[T](
    set: TagSet,
    initialSize: Int = 256
  ): TagMap[T] = new TagMap[T] { self =>
    require(initialSize >= 0)
    require(set.range.start >= 0)

    // while this a dependency on Netty 4, it is internal to this implementation
    // and not publicly exposed.
    private[this] val values = new IntObjectHashMap[T](initialSize)

    def map(el: T): Option[Int] = self.synchronized {
      set.acquire() match {
        case t@Some(tag) =>
          values.put(tag, el)
          t
        case None =>
          None
      }
    }

    def maybeRemap(tag: Int, newEl: T): Option[T] = {
      if (!inRange(tag)) return None

      self.synchronized {
        val oldEl = values.put(tag, newEl)
        Option(oldEl)
      }
    }

    def unmap(tag: Int): Option[T] = {
      if (!inRange(tag)) return None

      self.synchronized {
        val oldEl = values.remove(tag)
        val res = Option(oldEl)

        set.release(tag)
        res
      }
    }

    def get(tag: Int): Option[T] = {
      if (!inRange(tag)) return None

      self.synchronized {
        Option(values.get(tag))
      }
    }

    private[this] def inRange(tag: Int): Boolean =
      tag >= set.range.start && tag <= set.range.end

  }
}
