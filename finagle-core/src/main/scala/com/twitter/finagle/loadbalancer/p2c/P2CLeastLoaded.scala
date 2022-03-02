package com.twitter.finagle.loadbalancer.p2c

import com.twitter.finagle.loadbalancer.Balancer
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.LeastLoaded
import com.twitter.finagle.loadbalancer.Updating
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.ServiceFactoryProxy
import com.twitter.finagle.loadbalancer.LoadBalancerFactory.PanicMode
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
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
private[loadbalancer] final class P2CLeastLoaded[Req, Rep](
  protected val endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]],
  private[loadbalancer] val panicMode: PanicMode,
  protected val rng: Rng,
  protected val statsReceiver: StatsReceiver,
  protected val emptyException: NoBrokersAvailableException)
    extends Balancer[Req, Rep]
    with P2C[Req, Rep]
    with LeastLoaded[Req, Rep]
    with Updating[Req, Rep] {

  case class Node(factory: EndpointFactory[Req, Rep])
      extends ServiceFactoryProxy[Req, Rep](factory)
      with LeastLoadedNode

  protected def newNode(factory: EndpointFactory[Req, Rep]): Node = Node(factory)
}
