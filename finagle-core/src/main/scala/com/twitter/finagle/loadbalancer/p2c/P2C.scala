package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.Status
import com.twitter.finagle.util.Rng
import com.twitter.finagle.loadbalancer.{Balancer, DistributorT}

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
private[loadbalancer] trait P2C[Req, Rep] { self: Balancer[Req, Rep] =>
  /**
   * Our sturdy coin flipper.
   */
  protected def rng: Rng

  protected class Distributor(vector: Vector[Node])
    extends DistributorT[Node](vector) {
    type This = Distributor

    // There is nothing to rebuild (we don't partition in P2C) so we just return
    // `this` instance.
    def rebuild(): This = this
    def rebuild(vec: Vector[Node]): This = new Distributor(vec)

    def pick(): Node = {
      if (vector.isEmpty)
        return failingNode(emptyException)

      val size = vector.size

      if (size == 1) vector.head else {
        val a = rng.nextInt(size)
        var b = rng.nextInt(size - 1)
        if (b >= a) { b = b + 1 }

        val nodeA = vector(a)
        val nodeB = vector(b)

        // If both nodes are in the same health status, we pick the least loaded
        // one. Otherwise we pick the one that's healthier.
        if (nodeA.status == nodeB.status) {
          if (nodeA.load < nodeB.load) nodeA else nodeB
        } else {
          if (Status.best(nodeA.status, nodeB.status) == nodeA.status) nodeA else nodeB
        }
      }
    }

    // Since P2C is probabilistic in its selection, we don't partition and
    // select only from healthy nodes. Instead, we rely on the near zero
    // probability of selecting two down nodes (given the layers of retries
    // above us). However, namers can still force rebuilds when the underlying
    // set of nodes changes (eg: some of the nodes were unannounced and restarted).
    def needsRebuild: Boolean = false
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)
}
