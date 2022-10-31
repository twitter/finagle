package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.loadbalancer.Balancer
import com.twitter.finagle.loadbalancer.DistributorT
import com.twitter.finagle.stats.Verbosity
import com.twitter.finagle.util.Rng

/**
 * A mix-in that defines a [[DistributorT]] which uses [[P2CPick]].
 */
private[loadbalancer] trait P2C[Req, Rep] { self: Balancer[Req, Rep] =>

  /**
   * Our sturdy coin flipper.
   */
  protected def rng: Rng

  def additionalMetadata: Map[String, Any] = Map.empty

  protected class Distributor(vector: Vector[Node]) extends DistributorT[Node](vector) {
    type This = Distributor

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

    private[this] val p2cZeroCounter = self.statsReceiver.counter(
      description = "counts the number of times p2c selects two nodes with a zero load",
      Verbosity.ShortLived,
      "p2c",
      "zero"
    )

    def pick(): Node = {
      if (vector.isEmpty) failingNode
      else P2CPick.pick(vector, vector.size, self.rng, p2cZeroCounter)
    }
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)
}
