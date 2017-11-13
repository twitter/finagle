package com.twitter.finagle.mux.util

import io.netty.util.collection.IntObjectHashMap
import java.util.BitSet
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.Range
import scala.collection.mutable.ListBuffer

/**
 * A `TagMap` maintains a mapping between tags and elements of type `T`.
 *
 * @note it is up to the user to ensure thread safety of the `TagMap` unless
 *       otherwise noted in the implementation.
 */
private[mux] trait TagMap[T] {

  /**
   * Number of tags mapped by this `TagMap`.
   */
  def size: Int

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
   * Remove all elements from the `TagMap`, returning the active elements
   * as a collection.
   */
  def unmapAll(): Seq[T]
}

private[mux] object TagMap {

  def apply[T](range: Range, initialSize: Int): TagMap[T] = {
    val set = new TagSet(range)
    new TagMapImpl[T](set, initialSize)
  }

  // Instances of TagMapImpl assume ownership of the `TagSet` passed in the constructor
  private[this] class TagMapImpl[T](set: TagSet, initialSize: Int) extends TagMap[T] { self =>
    require(initialSize >= 0)
    require(set.range.start >= 0)

    def size: Int = values.size

    // while this a dependency on Netty 4, it is internal to this implementation
    // and not publicly exposed.
    private[this] val values = new IntObjectHashMap[T](initialSize)

    def map(el: T): Option[Int] = {
      set.acquire() match {
        case t @ Some(tag) =>
          values.put(tag, el)
          t
        case None =>
          None
      }
    }

    def maybeRemap(tag: Int, newEl: T): Option[T] = {
      if (!inRange(tag)) {
        None
      } else {
        val oldEl = values.put(tag, newEl)
        Option(oldEl)
      }
    }

    def unmap(tag: Int): Option[T] = {
      if (!inRange(tag)) {
        None
      } else {
        val oldEl = values.remove(tag)
        val res = Option(oldEl)

        set.release(tag)
        res
      }
    }

    def get(tag: Int): Option[T] = {
      if (!inRange(tag)) None
      else Option(values.get(tag))
    }

    def unmapAll(): Seq[T] = {
      val results = values.values().asScala.toList
      set.clear()
      values.clear()
      results
    }

    private[this] def inRange(tag: Int): Boolean =
      tag >= set.range.start && tag <= set.range.end

    override def toString: String =
      values.keySet.asScala.mkString("TagMap[", ", ", "]")
  }

  /**
   * `TagSet` maintains a mutable set of tags (integers)
   * within a specified range. TagSets reuses smaller available
   * tags before issuing larger ones.
   *
   * @note `TagSet` is not thread-safe.
   */
  // visible for testing
  private[util] final class TagSet(val range: Range) {
    // We could easily stripe the bitsets here, since we don't
    // require contiguous tag assignment.
    require(range.step == 1)
    private[this] val start = range.start
    private[this] val bits = new BitSet

    /** Acquire a tag, if available */
    def acquire(): Option[Int] = {
      val tag = bits.nextClearBit(start)
      if (!range.contains(tag)) {
        None
      } else {
        bits.set(tag)
        Some(tag)
      }
    }

    /** Remove all tags from the `TagSet` */
    def clear(): Unit = {
      bits.clear()
    }

    /** Release a previously acquired tag */
    def release(tag: Int): Unit = {
      // TODO: should we worry about releasing clear
      // or out-of-range bits?
      bits.clear(tag)
    }

    /** Create a snapshot of the currently acquired tags */
    def toSeq: Seq[Int] = {
      @tailrec
      def build(i: Int, buffer: ListBuffer[Int]): Seq[Int] = {
        val next = bits.nextSetBit(i)
        if (next == -1) {
          buffer.result()
        } else {
          buffer += next
          build(next + 1, buffer)
        }
      }

      build(0, new ListBuffer)
    }
  }
}
