package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.Status
import com.twitter.finagle.util.Rng
import com.twitter.finagle.loadbalancer.{DistributorT, NodeT}

/**
 * A mix-in for a [[DistributorT]] that uses the ideas behind the
 * "power of 2 choices" [1] to select two nodes from the underlying
 * vector.
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
private[loadbalancer] trait P2CPick[Node <: NodeT[_, _]] { self: DistributorT[Node] =>

  /**
   * The random number generator used by `pick` to select two nodes
   * for comparison.
   */
  protected def rng: Rng

  /**
   * The Node returned from `pick` when `vector` is empty.
   */
  protected def emptyNode: Node

  /**
   * The upper bound (exclusive) over which `pick` selects
   * nodes for comparison. Note, this should be <= vector.size.
   */
  protected def bound: Int

  /**
   * Allows implementations to pre-process the `vector` which
   * `pick` will select from.
   */
  protected val vec: Vector[Node]

  /**
   * Picks two nodes randomly, and uniformly, from `vector` within `bound` and
   * selects between the two first by `status` and then by `load`. Effectively,
   * we want to select the most healthy, least loaded of the two.
   */
  def pick(): Node = {
    val range = bound
    if (vec.isEmpty) emptyNode
    else if (range == 1 || vec.size == 1) vec.head
    else {
      // We want to pick two distinct nodes. We do this without replacement by
      // restricting the bounds of the second selection, `b`, to be one less than the
      // the first, `a`. Effectively, we are creating a "hole" in the second selection.
      // We avoid the "hole" by incrementing the index by one when we hit it. However,
      // special care must be taken to not bias towards "hole" + 1, so we treat the
      // entire range greater than "hole" uniformly (hence, the >= in the collision
      // comparison).
      val a = rng.nextInt(range)
      var b = rng.nextInt(range - 1)
      if (b >= a) { b = b + 1 }

      val nodeA = vec(a)
      val nodeB = vec(b)

      // If both nodes are in the same health status, we pick the least loaded
      // one. Otherwise we pick the one that's healthier.
      val aStatus = nodeA.status
      val bStatus = nodeB.status
      if (aStatus == bStatus) {
        if (nodeA.load <= nodeB.load) nodeA else nodeB
      } else {
        if (Status.best(aStatus, bStatus) == aStatus) nodeA else nodeB
      }
    }
  }
}
