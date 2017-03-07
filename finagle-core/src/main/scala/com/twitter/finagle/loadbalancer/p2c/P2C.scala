package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.loadbalancer.{Balancer, DistributorT}
import com.twitter.finagle.util.Rng

/**
 * A mix-in that defines a [[DistributorT]] which uses [[P2CPick]].
 */
private[loadbalancer] trait P2C[Req, Rep] { self: Balancer[Req, Rep] =>
  /**
   * Our sturdy coin flipper.
   */
  protected def rng: Rng

  protected class Distributor(vector: Vector[Node])
    extends DistributorT[Node](vector)
    with P2CPick[Node] {
    type This = Distributor

    protected def bound: Int = vector.size
    protected def emptyNode = failingNode(emptyException)
    protected def rng = self.rng

    // There is nothing to rebuild (we don't partition in P2C) so we just return
    // `this` instance.
    def rebuild(): This = this
    def rebuild(vec: Vector[Node]): This = new Distributor(vec)

    // Since P2C is probabilistic in its selection, we don't partition and
    // select only from healthy nodes. Instead, we rely on the near zero
    // probability of selecting two down nodes (given the layers of retries
    // above us). However, namers can still force rebuilds when the underlying
    // set of nodes changes (eg: some of the nodes were unannounced and restarted).
    def needsRebuild: Boolean = false
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)
}