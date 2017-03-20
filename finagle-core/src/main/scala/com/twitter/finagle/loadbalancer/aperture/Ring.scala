package com.twitter.finagle.loadbalancer.aperture

/**
 * Ring allows us to map indices uniformly across a ring topology. An index
 * can be queried via the `apply` method. Given a position on the ring, various
 * [[Iterator Iterators]] are available that traverse the ring. These traversals
 * are useful to derive an ordering for servers and are used by [[Aperture]].
 *
 * @param numIndices The number of indices mapped on to the ring.
 */
private class Ring(numIndices: Int) { self =>
  require(numIndices > 0, s"numIndices must be positive: $numIndices")

  /**
   * Returns the number of indices that are part of the ring.
   */
  def size: Int = numIndices

  /**
   * Returns the (zero-based) index which maps to the position
   * on the ring that is closest to the given `pos`.
   *
   * @param pos A position on the ring between [-1.0, 1.0].
   * Negative values are allowed and signify traversing the
   * ring counter clockwise.
   */
  def apply(pos: Double): Int = {
    require(pos >= -1.0D && pos <= 1.0D, s"pos must be >= -1.0 and <= 1.0: $pos")
    val idx: Int = math.round(pos * size).toInt % size
    if (idx < 0) idx + size else idx
  }

  /**
   * Returns an [[Iterator]] which walks the ring clockwise,
   * one full rotation, starting at position `pos`.
   *
   * @example An example iteration:
   * {{{
   * val ring = new Ring(3)
   * val iter = ring.iter(0.5)
   * iter.next() // => 2
   * iter.next() // => 0
   * iter.next() // => 1
   * iter.hasNext // => false
   * }}}
   *
   * @param pos A position on the ring between [-1.0, 1.0].
   */
  def iter(pos: Double): Iterator[Int] =
    new Iterator[Int] {
      private[this] var numVisited = 0
      private[this] var index = self(pos)

      def hasNext: Boolean = numVisited < self.size
      def next(): Int = {
        if (!hasNext) throw new NoSuchElementException

        val idx = index % self.size
        index += 1
        numVisited += 1
        idx
      }
    }

  /**
   * Returns an [[Iterator]] which walks this ring bidirectionally,
   * one full rotation, starting at position `pos`. The iterator alternates
   * clockwise and counter clockwise on the ring.
   *
   * @example An example iteration:
   * {{{
   * val ring = new Ring(3)
   * val iter = ring.alternatingIter(0.5)
   * iter.next() // => 2
   * iter.next() // => 1
   * iter.next() // => 0
   * iter.hasNext // => false
   * }}}
   *
   * @param pos A position on the ring between [-1.0, 1.0].
   */
  def alternatingIter(pos: Double): Iterator[Int] =
    new Iterator[Int] {
      private[this] var numVisited = 0
      private[this] var clockwise = self(pos)
      private[this] var counter = clockwise - 1

      def hasNext: Boolean = numVisited < self.size
      def next(): Int = {
        if (!hasNext) throw new NoSuchElementException

        val idx = if (numVisited % 2 == 0) {
          val i = clockwise % self.size
          clockwise += 1
          i
        } else {
          val i = counter % self.size
          counter -= 1
          i
        }

        numVisited += 1
        if (idx < 0) idx + self.size else idx
      }
    }
}