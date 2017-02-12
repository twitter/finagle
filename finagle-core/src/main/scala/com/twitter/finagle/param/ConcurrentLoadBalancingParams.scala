package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.loadbalancer.ConcurrentLoadBalancerFactory

/**
 * A collection of methods for configuring the Load Balancing (concurrent) module
 * of Finagle clients.
 *
 * A concurrent load balancer balancers the traffic among multiple connections, which
 * is useful for maintaining concurrency in pipelining protocols.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing]]
 */
class ConcurrentLoadBalancingParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A])
  extends DefaultLoadBalancingParams(self) {

  /**
   * Configures the number of concurrent `connections` a single endpoint has
   * (default: 4).
   *
   * Each endpoint can open multiple connections. For `N` endpoints, each opens
   * `connections`, load balancer balances among `N * connections` options. Thus,
   * it increases concurrency of each endpoint.
   */
  def connectionsPerEndpoint(connections: Int): A =
    self.configured(ConcurrentLoadBalancerFactory.Param(connections))
}
