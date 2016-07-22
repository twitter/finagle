package com.twitter.finagle.loadbalancer

import com.twitter.finagle.Status
import com.twitter.finagle.util.Rng

/**
 * An O(1), concurrent, weighted fair load balancer. This uses the
 * ideas behind "power of 2 choices" [1] combined with O(1) biased
 * coin flipping through the aliasing method, described in
 * [[com.twitter.finagle.util.Drv Drv]].
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
private trait P2C[Req, Rep] { self: Balancer[Req, Rep] =>
  /**
   * Our sturdy coin flipper.
   */
  protected def rng: Rng

  protected class Distributor(vector: Vector[Node])
    extends DistributorT[Node](vector) {
    type This = Distributor

    def rebuild(): This = new Distributor(vector)
    def rebuild(vec: Vector[Node]): This = new Distributor(vec)

    def pick(): Node = {
      if (selections.isEmpty)
        return failingNode(emptyException)

      val size = selections.size

      if (size == 1) selections.head else {
        val a = rng.nextInt(size)
        var b = rng.nextInt(size)

        // Try to pick b, b != a, up to 10 times.
        var i = 10
        while (a == b && i > 0) {
          b = rng.nextInt(size)
          i -= 1
        }

        val nodeA = selections(a)
        val nodeB = selections(b)

        if (nodeA.status != Status.Open || nodeB.status != Status.Open)
          sawDown = true

        if (nodeA.load < nodeB.load) nodeA else nodeB
      }
    }
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)
}
