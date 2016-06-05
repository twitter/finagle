package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withLoadBalancer` (concurrent balancer) API entry point.
 *
 * @see [[ConcurrentLoadBalancingParams]]
 */
trait WithConcurrentLoadBalancer[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client's load balancer that implements
   * a strategy for choosing one host/node from a replica set to service
   * a request.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing]]
   */
  val withLoadBalancer: ConcurrentLoadBalancingParams[A] = new ConcurrentLoadBalancingParams(self)
}
