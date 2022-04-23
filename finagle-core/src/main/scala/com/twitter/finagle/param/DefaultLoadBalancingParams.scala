package com.twitter.finagle.param

import com.twitter.finagle.Stack
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.loadbalancer.PanicMode

/**
 * A collection of methods for configuring the Load Balancing (default) module
 * of Finagle clients.
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 *
 * @see [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing]]
 */
class DefaultLoadBalancingParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Configures this client with a given [[LoadBalancerFactory load balancer]] that
   * implements a strategy for choosing one host/node from a replica set to service
   * a request.
   *
   * The default setup for a Finagle client is to use
   * [[com.twitter.finagle.loadbalancer.Balancers.p2c power of two choices]] algorithm
   * to distribute load across endpoints, while picking the least loaded one.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing]]
   */
  def apply(loadBalancer: LoadBalancerFactory): A =
    self.configured(LoadBalancerFactory.Param(loadBalancer))

  /**
   * Enables the probation mode for the current load balancer (default: disabled).
   *
   * When enabled, the balancer treats removals as advisory and flags them. If a
   * a flagged endpoint is also detected as unhealthy by a circuit breaker (e.g.
   * fail-fast, failure accrual, etc) then the host is removed from the collection.
   *
   * Put differently, this allows the client to have a soft dependency on the source
   * of its replica set. The client maintains stale entries as long as they are healthy
   * from its perspective.
   *
   * @see [[https://twitter.github.io/finagle/guide/Clients.html#load-balancing]]
   */
  def probation: A =
    self.configured(LoadBalancerFactory.EnableProbation(enable = true))

  /**
   * Configures load balancer to "panic" when the percentage of unhealthy nodes
   * reaches the panicThreshold. Load balancer will send some load to unhealthy
   * nodes. Panic mode is probabilistic and per-request. Disabled for heap load
   * balancer. Default is PanicMode.MajorityUnhealthy
   * @param panicThreshold a threshold percent of unhealthy nodes tolerated.
   * When threshold is reached, the lB panics for that request. The higher the
   * threshold, the more attempts the load balancer will retry. Note that these
   * percents are estimates. They are calculated for P2C* and Aperture* LBs.
   * For round robin, these percents do not apply because it is not a pick two
   * based algorithm.
   */
  def panicMode(panicThreshold: PanicMode): A =
    self.configured(panicThreshold)
}
