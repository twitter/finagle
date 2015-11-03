package com.twitter.finagle.mux.util

import com.twitter.finagle.mux.transport.Message
import java.util.BitSet
import scala.collection.immutable.Range

/**
 * Trait TagSet maintains a mutable set of tags (integers)
 * within a specified range. TagSets reuses smaller available
 * tags before issuing larger ones.
 */
private[mux] trait TagSet extends Iterable[Int] {
  /** The range of tags maintained by this TagSet */
  val range: Range
  /** Acquire a tag, if available */
  def acquire(): Option[Int]
  /** Release a previously acquired tag */
  def release(tag: Int)
}

private[mux] object TagSet {
  /**
   * Constructs a space-efficient TagSet for the range of available
   * tags in the mux protocol.
   */
  def apply(): TagSet = TagSet(Message.MinTag to Message.MaxTag)

  /** Constructs a space-efficient TagSet for the given range */
  def apply(_range: Range): TagSet = new TagSet {
    val range = _range
    // We could easily stripe the bitsets here, since we don't
    // require contiguous tag assignment.
    require(range.step == 1)
    val start = range.start
    val bits = new BitSet

    def acquire(): Option[Int] = synchronized {
      val tag = bits.nextClearBit(start)
      if (!range.contains(tag)) None else {
        bits.set(tag)
        Some(tag)
      }
    }

    def release(tag: Int): Unit = synchronized {
      // TODO: should we worry about releasing clear
      // or out-of-range bits?
      bits.clear(tag)
    }

    def iterator: Iterator[Int] = new Iterator[Int] {
      var _next = start-1
      next()

      def hasNext = _next != -1
      def next() = {
        val cur = _next
        _next = TagSet.this.synchronized(bits.nextSetBit(_next+1))
        cur
      }
    }

    // for performance
    override def isEmpty: Boolean = synchronized { bits.isEmpty }
  }
}
