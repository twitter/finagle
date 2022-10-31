package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.Status
import com.twitter.finagle.util.Rng
import com.twitter.finagle.loadbalancer.NodeT
import com.twitter.finagle.stats.Counter

/**
 * A helper that uses the ideas behind the "power of 2 choices"
 * [1] to select two nodes from the underlying vector.
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
private[loadbalancer] object P2CPick {

  /**
   * Picks two nodes randomly, and uniformly, from `vector` within `bound` and
   * selects between the two first by `status` and then by `load`. Effectively,
   * we want to select the most healthy, least loaded of the two.
   */
  def pick[Node <: NodeT[_, _]](
    vec: IndexedSeq[Node],
    range: Int,
    rng: Rng,
    p2cZeroCounter: Counter
  ): Node = {
    assert(vec.nonEmpty)

    if (range == 1 || vec.size == 1) vec.head
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

      // We measure the effectiveness of our load metric by comparing the load of the
      // two nodes. In cases like least loaded, an ineffective load metric, where the client
      // lacks enough concurrency, would be zero for both nodes.
      if (nodeA.load == 0 && nodeB.load == 0) {
        p2cZeroCounter.incr()
      }

      if (aStatus == bStatus) {
        if (nodeA.load <= nodeB.load) nodeA else nodeB
      } else {
        if (Status.best(aStatus, bStatus) == aStatus) nodeA else nodeB
      }
    }
  }
}
