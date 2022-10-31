package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Status
import com.twitter.finagle.loadbalancer.NodeT
import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Level
import com.twitter.logging.Logger

/**
 * A node-picker object that uses the ideas behind the
 * "power of 2 choices" [1] to select two nodes from a given
 * [[ProbabilityDistribution]]
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
private object WeightedP2CPick {

  /**
   * Pick the least loaded (and healthiest) of the two nodes `a` and `b`
   * taking into account their respective weights.
   */
  private[this] def pick[Node <: NodeT[_, _]](a: Node, aw: Double, b: Node, bw: Double): Node = {
    val aStatus = a.status
    val bStatus = b.status
    if (aStatus == bStatus) {
      // Note, `aw` or `bw` can't be zero since `pick2` would not
      // have returned the indices in the first place. However,
      // we check anyways to safeguard against any numerical
      // stability issues.
      val _aw = if (aw == 0) 1.0 else aw
      val _bw = if (bw == 0) 1.0 else bw

      // We first check if the weights are effectively the same so we can
      // ignore them if they are. If we just go for it and use them we can
      // evaluate (1.0 / 1.0 <= 1.0 / 1.0000000001) and we bias toward the
      // second instance which should have identical weight but it's a hair
      // off due to floating point precision errors.
      if (approxEqual(_aw, _bw)) {
        if (a.load <= b.load) a else b
      } else {
        if (a.load / _aw <= b.load / _bw) a else b
      }
    } else {
      if (Status.best(aStatus, bStatus) == aStatus) a else b
    }
  }

  // A quick helper for seeing if a is close to the same value as b.
  // This allows for avoiding bias due to numerical instability in
  // floating point values.
  private[this] def approxEqual(a: Double, b: Double): Boolean =
    math.abs(a - b) < 0.0001

  /**
   * Picks two nodes randomly, and uniformly, from `vector` within `bound` and
   * selects between the two first by `status` and then by `load`. Effectively,
   * we want to select the most healthy, least loaded of the two.
   */
  def pick[Node <: NodeT[_, _]](
    pdist: ProbabilityDistribution[Node],
    logger: Logger = null,
    p2cZeroCounter: Counter = NullStatsReceiver.NullCounter
  ): Node = {
    val a = pdist.pickOne()
    val b = pdist.tryPickSecond(a)
    val aw = pdist.weight(a)
    val bw = pdist.weight(b)

    val nodeA = pdist.get(a)
    val nodeB = pdist.get(b)

    // We measure the effectiveness of our load metric by comparing the load of the
    // two nodes. In cases like least loaded, an ineffective load metric, where the client
    // lacks enough concurrency, would be zero for both nodes.
    if (nodeA.load == 0 && nodeB.load == 0) {
      p2cZeroCounter.incr()
    }

    val picked = pick(nodeA, aw, nodeB, bw)

    if (logger != null && logger.isLoggable(Level.TRACE)) {
      logger.trace(
        f"[WeightedP2C.pick] a=(index=$a, weight=$aw%1.6f, node=$nodeA) b=(index=$b, weight=$bw%1.6f, node=$nodeB) picked=$picked"
      )
    }

    picked
  }

}
