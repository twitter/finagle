package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.loadbalancer.{Balancer, LeastLoaded, Updating}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.finagle.{NoBrokersAvailableException, ServiceFactory}
import com.twitter.util.Activity

/**
 * An O(1), concurrent, least-loaded fair load balancer. This uses the ideas
 * behind "power of 2 choices" [1].
 *
 * @param endpoints An activity that updates with the set of nodes over which
 * we distribute load.
 *
 * @param maxEffort the maximum amount of "effort" we're willing to
 * expend on a load balancing decision without reweighing.
 *
 * @param rng The PRNG used for flipping coins. Override for
 * deterministic tests.
 *
 * @param statsReceiver The stats receiver to which operational
 * statistics are reported.
 *
 * [1] Michael Mitzenmacher. 2001. The Power of Two Choices in
 * Randomized Load Balancing. IEEE Trans. Parallel Distrib. Syst. 12,
 * 10 (October 2001), 1094-1104.
 */
private[loadbalancer] class P2CLeastLoaded[Req, Rep](
    protected val endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
    protected val maxEffort: Int,
    protected val rng: Rng,
    protected val statsReceiver: StatsReceiver,
    protected val emptyException: NoBrokersAvailableException)
  extends Balancer[Req, Rep]
  with LeastLoaded[Req, Rep]
  with P2C[Req, Rep]
  with Updating[Req, Rep] {
  protected[this] val maxEffortExhausted = statsReceiver.counter("max_effort_exhausted")
}