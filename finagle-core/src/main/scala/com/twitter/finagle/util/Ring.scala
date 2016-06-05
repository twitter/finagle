package com.twitter.finagle.util

import java.util.Arrays.binarySearch

/**
 * Class `Ring` implements a hash ring. Given an array mapping
 * indices to positions, the ring supplies the reverse mapping: to
 * which index does a position belong?
 *
 * Its distinguishing feature is that `Ring` can pick random
 * positions in a range. (And then map them to their corresponding
 * indices.) Ring can also pick without replacement two elements from
 * a range.
 *
 * {{{
 * val r = new Ring(Array(1,5,20))
 * r(0)  == 0
 * r(1)  == 0
 * r(2)  == 1
 * r(5)  == 1
 * r(6)  == 2
 * r(20) == 2
 * r(21) == 0 // Wraps around; it's a ring!
 * }}}
 */
class Ring(positions: Array[Int]) {
  require(positions.nonEmpty)
  private[this] val N = positions.length

  // An internal array, of size 2N, which is extended to repeat each
  // position: it is positions concatenated with itself, with
  // values adjusted in the second half.
  //
  // This allows us to implement ring operations trivially without
  // having to worry about performing modular arithmetic.
  //
  // We also avoid overflow issues by using Longs to represent
  // positions.
  private[this] val nodes: Array[Long] = {
    val n = new Array[Long](N*2)
    for (i <- positions.indices) {
      n(i) = positions(i).toLong
      n(i+N) = positions.last + positions(i).toLong
    }
    n
  }

  /**
   * Compute the index of the given position.
   */
  private[this] def index(pos: Long): Int = {
    // We don't search over the entire range here in case the position
    // is out of bounds, which in any case would snap to the last index.
    var i = binarySearch(nodes, 0, N*2, pos) match {
      case i if i < 0 => -1-i
      case i => i
    }

    // In the case where positions overlap, we always
    // select the first one. This is to support zero weights.
    while (i > 0 && nodes(i) == nodes(i-1))
      i -= 1

    i
  }

  /**
   * Compute the width of the given index.
   */
  private[this] def width(i: Int): Int =
    if (i == 0) nodes(0).toInt
    else (nodes(i) - nodes(i-1)).toInt

  /**
   * Compute the range of the given index.
   */
  private[this] def range(i: Int): (Long, Long) =
    (nodes(i)-width(i), nodes(i))

  /**
   * Compute the amount of intersection between the two ranges.
   */
  private[this] def intersect(b0: Long, e0: Long, b1: Long, e1: Long): Int =
    math.max(0, (math.min(e0, e1) - math.max(b0, b1))).toInt

  /**
   * Compute the index of the given position.
   */
  def apply(pos: Int): Int =
    index(pos)%N

  /**
   * Pick a position within the given range, described
   * by an offset and width, returning the index of the
   * picked element.
   *
   * @param rng The [[Rng]] used for picking.
   * @param off The offset from which to pick.
   * @param wid The width of the range.
   */
  def pick(rng: Rng, off: Int, wid: Int): Int =
    index(off.toLong + rng.nextLong(wid))%N

  /**
   * Pick two elements. They are picked without replacement as long as
   * the offset and width admits it. Zero-width nodes cannot be picked
   * without replacement.
   *
   * @param rng The [[Rng]] used for picking.
   * @param off The offset from which to pick.
   * @param wid The width of the range.
   */
  def pick2(rng: Rng, off: Int, wid: Int): (Int, Int) = {
    // We pick element a from the full range [off, off+wid); element b
    // is picked from "piecewise" range we get by subtracting the range
    // of a, i.e.: [off, begin(a)), [end(a), off+wid).
    val a = index(off.toLong + rng.nextLong(wid))
    val (ab, ae) = range(a)
    val discount = intersect(off, off.toLong+wid.toLong, ab, ae)

    val wid1 = wid - discount
    if (wid1 == 0)
      return (a, a)

    // Instead of actually splitting the range into two, we offset
    // any pick that takes place in the second range if there is a
    // possibility that our second choice falls within [ab, ae].
    //
    // There are three cases which we need to consider:
    // 1. [ab, ae] is a proper subset of [off, off+wid1]
    // 2. ab is outside of [off, off+wid1] but ae is inside of it.
    // 3. ab is inside of [off, off+wid1] but ae falls outside of it.
    //
    // To handle #1, #2, we verify that our random selection is
    // greater than or equal to the start of our intersection
    // and bump it by our overlap.
    //
    // #3 is taken care of by the fact that we've discounted any
    // possible tail overlap from `wid` in `wid1`.
    var pos = off.toLong + rng.nextLong(wid1)
    if (pos >= ae-discount)
      pos += discount

    (a%N, index(pos)%N)
  }
}

object Ring {
  /**
   * Returns a ring with `numSlices` slices and width `width`.
   */
  def apply(numSlices: Int, width: Int): Ring = {
    require(numSlices > 0)
    require(width >= numSlices, "ring not wide enough")
    val unit = width/numSlices.toDouble
    val positions = Array.tabulate(numSlices) { i => ((i+1)*unit).toInt }
    new Ring(positions)
  }
}
