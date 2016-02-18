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

  protected class Distributor(val vector: Vector[Node])
    extends DistributorT[Node] {
    type This = Distributor

    // Indicates if we've seen any down nodes during pick which we expected to be available
    @volatile private[this] var sawDown = false

    private[this] val nodeUp: Node => Boolean = { node =>
      node.status == Status.Open
    }

    private[this] val (up, down) = vector.partition(nodeUp)

    def needsRebuild: Boolean =
      sawDown || (down.nonEmpty && down.exists(nodeUp))

    def rebuild(): This = new Distributor(vector)
    def rebuild(vec: Vector[Node]): This = new Distributor(vec)

    // TODO: consider consolidating some of this code with `Aperture.Distributor.pick`
    def pick(): Node = {
      if (vector.isEmpty)
        return failingNode(emptyException)

      // if all nodes are down, we might as well try to send requests somewhere
      // as our view of the world may be out of date.
      val vec = if (up.isEmpty) down else up
      val size = vec.size

      if (size == 1) vec.head else {
        val a = rng.nextInt(size)
        var b = rng.nextInt(size)

        // Try to pick b, b != a, up to 10 times.
        var i = 10
        while (a == b && i > 0) {
          b = rng.nextInt(size)
          i -= 1
        }

        val nodeA = vec(a)
        val nodeB = vec(b)

        if (nodeA.status != Status.Open || nodeB.status != Status.Open)
          sawDown = true

        if (nodeA.load < nodeB.load) nodeA else nodeB
      }
    }
  }

  protected def initDistributor() = new Distributor(Vector.empty)
}
