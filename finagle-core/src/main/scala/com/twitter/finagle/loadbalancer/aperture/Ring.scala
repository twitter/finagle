package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.util.Rng
import scala.collection.immutable.VectorBuilder

private object Ring {

  /**
   * Returns the length of the intersection between the two ranges.
   *
   * @note this implementations assumes that the min(e0, e1) is greater
   * than max(b0, b1). It's up to the caller to handle the case where
   * the line segments wrap around the ring.
   */
  def intersect(b0: Double, e0: Double, b1: Double, e1: Double): Double = {
    val len = math.min(e0, e1) - math.max(b0, b1)
    math.max(0.0, len)
  }
}

/**
 * Ring maps the indices [0, `size`) uniformly around a coordinate space [0.0, 1.0).
 *
 * It then provides methods for querying the indices across ranges (in the same
 * coordinate space) which the [[Aperture]] load balancer uses to calculate which
 * servers a respective client will talk to. See [[ProcessCoordinate]] for more
 * details about how clients compute their ranges which map into an instance of
 * [[Ring]].
 *
 * @param size the number of indices mapped on the ring.
 *
 * @param rng the random number generator used for `pick` and `pick2`.
 */
private class Ring(size: Int, rng: Rng) {
  import Ring._

  require(size > 0, s"size must be > 0: $size")

  /**
   * Returns the uniform width of any given index on the ring. The returned
   * value is bounded between (0, 1].
   */
  val unitWidth: Double = 1.0 / size

  /**
   * Returns the (zero-based) index between [0, `size`) which the
   * position `offset` maps to.
   *
   * @param offset A value between [0, 1.0).
   */
  def index(offset: Double): Int = {
    if (offset < 0 || offset >= 1.0)
      throw new IllegalArgumentException(s"offset must be between [0, 1.0): $offset")

    math.floor(offset * size).toInt % size
  }

  /**
   * Returns the total number of indices that [offset, offset + width) intersects with.
   *
   * @note This returns the number of indices over which `pick` and `pick2` select.
   * Thus, we interpret a width of 0 as picking one index.
   */
  def range(offset: Double, width: Double): Int = {
    if (width < 0 || width > 1.0) {
      throw new IllegalArgumentException(s"width must be between [0, 1.0]: $width")
    } else if (width < 1.0) {
      val begin = index(offset)
      val end = index((offset + width) % 1.0)

      // We wrapped around the entire ring, so return the size.
      if (begin == end && width > unitWidth) size
      // We only have one index to select from. Arguably, returning
      // a diff of zero here is correct too. However, in order to
      // project what `pick2` will do we return a range of 1.
      else if (begin == end) 1
      else {
        val beginWeight = weight(begin, offset, width)
        val endWeight = weight(end, offset, width)

        // we want to project what `pick2` will do, so we need to
        // take into account the weight of `begin` and `end`.
        val adjustedBegin = if (beginWeight > 0) begin else begin + 1
        val adjustedEnd = if (endWeight > 0) end + 1 else end

        val diff = adjustedEnd - adjustedBegin
        if (diff <= 0) diff + size else diff
      }
    } else {
      // We know that `width == 1.0` in this case, meaning the entire
      // ring is within range.
      size
    }
  }

  /**
   * Returns the ratio of the intersection between `index` and [offset, offset + width).
   */
  def weight(index: Int, offset: Double, width: Double): Double = {
    if (index >= size)
      throw new IllegalArgumentException(s"index must be < size: $index")
    if (width < 0 || width > 1.0)
      throw new IllegalArgumentException(s"width must be between [0, 1.0]: $width")

    val ab = index * unitWidth
    val ae = ab + unitWidth

    // In cases where [offset, offset + width) wraps around the ring,
    // it's easier to calculate the size of the inverse range,
    // and subtract that from the size of the full ring
    if (offset + width > 1.0) {
      // We know that the inverse of [offset, offset + width) is a single
      // contiguous range, which does not overlap the boundary of the ring.
      val start = (offset + width) % 1
      val end = offset

      // 1.0 is the size of the full ring, so by subtracting the size of the inverse,
      // we can determine the size of [offset, offset + width)
      1D - (intersect(ab, ae, start, end) / unitWidth)
    } else {
      intersect(ab, ae, offset, offset + width) / unitWidth
    }
  }

  /**
   * Returns the indices where [offset, offset + width) intersects.
   *
   * @note This returns the indices over which `pick` and `pick2` select.
   * Thus, we interpret a width of 0 as picking one index.
   */
  def indices(offset: Double, width: Double): Seq[Int] = {
    val builder = new VectorBuilder[Int]
    var i = index(offset)
    var r = range(offset, width)
    while (r > 0) {
      val idx = i % size
      builder += idx
      i += 1
      r -= 1
    }
    builder.result()
  }

  /**
   * Pick a random index between [0, `size`) where the range of the
   * index intersects with [offset, offset + width).
   *
   * @param width The width of the range. We interpret a width of 0 as the range
   * [offset, offset] and as such return a valid index.
   */
  def pick(offset: Double, width: Double): Int = {
    if (width < 0 || width > 1.0)
      throw new IllegalArgumentException(s"width must be between [0, 1.0]: $width")

    index((offset + (rng.nextDouble() * width)) % 1.0)
  }

  /**
   * Picks a random index between [0, `size`) where the positions for the
   * respective index intersect with [offset, offset + width), so long as
   * the index is not `a` (if the range permits it).
   *
   * @note we expose this outside of `pick2` so that we can avoid a tuple
   * allocation on the hot path.
   */
  def tryPickSecond(a: Int, offset: Double, width: Double): Int = {
    // Element `b` is picked from "piecewise" range we get by subtracting
    // the range of a, i.e.: [offset, ab), [ae, offset + width).
    // In cases where `offset + width` wraps around the ring, we need
    // to scale the range by 1.0 where it overlaps.
    val ab: Double = {
      val ab0 = (a * unitWidth)
      if (ab0 + 1 < offset + width) ab0 + 1 else ab0
    }
    val ae: Double = ab + unitWidth

    val overlap = intersect(ab, ae, offset, offset + width)
    val rem = width - overlap

    if (rem > 0) {
      // Instead of actually splitting the range into two, we offset
      // any pick that takes place in the second range if there is a
      // possibility that our second choice falls within [ab, ae].
      //
      // Note, special care must be taken to not bias towards ae + overlap, so
      // we treat the entire range greater than it uniformly.
      var pos = offset + (rng.nextDouble() * rem)
      if (pos >= ae - overlap) { pos += overlap }
      index(pos % 1.0)
    } else {
      // The range [offset, offset + width) is equivalent to [ab, ae).
      a
    }
  }

  /**
   * Picks two random indices between [0, `size`) where the positions for the
   * respective indices intersect with [offset, offset + width). The indices are
   * chosen uniformly and without replacement.
   *
   * @param width The width of the range. We interpret a width of 0 as the range
   * [offset, offset] and as such return a valid index.
   */
  def pick2(offset: Double, width: Double): (Int, Int) = {
    val a = pick(offset, width)
    val b = tryPickSecond(a, offset, width)
    (a, b)
  }
}
