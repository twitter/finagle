package com.twitter.finagle.param

import com.twitter.finagle.Stack

/**
 * Provides the `withLoadBalancer` (default balancer) API entry point.
 *
 * @see [[DefaultLoadBalancingParams]]
 */
trait WithDefaultLoadBalancer[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring the client's load balancer that implements
   * a strategy for choosing one host/node from a replica set to service
   * a request.
   *
   * The default setup for a Finagle client is to use power of two choices
   * algorithm to distribute load across endpoints, and comparing nodes
   * via a least loaded metric.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing]]
   */
  val withLoadBalancer: DefaultLoadBalancingParams[A] = new DefaultLoadBalancingParams(self)
}
