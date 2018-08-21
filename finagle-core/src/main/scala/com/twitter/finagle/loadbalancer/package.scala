package com.twitter.finagle
import scala.util.hashing.MurmurHash3

/**
 * This package implements client side load balancing algorithms.
 *
 * As an end-user, see the [[Balancers]] API to create instances which can be
 * used to configure a Finagle client with various load balancing strategies.
 *
 * As an implementor, each algorithm gets its own subdirectory and is exposed
 * via the [[Balancers]] object. Several convenient traits are provided which factor
 * out common behavior and can be mixed in (i.e. Balancer, DistributorT, NodeT,
 * and Updating).
 */
package object loadbalancer {

  /**
   * A reference to the current address [[Ordering]]. By default, it orders
   * [[Address Addresses]] based on a deterministic hash of their IP.
   *
   * @note In the case of unresolved addresses, certain sorting implementations
   * will require consistent results across comparisons so it may fail
   * during the sort.
   */
  @volatile private[this] var addressOrdering: Ordering[Address] =
    Address.hashOrdering(MurmurHash3.arraySeed)

  /**
   * Set the default [[Address]] ordering for the entire process (outside of clients
   * which override it).
   *
   * @see [[LoadBalancerFactory.AddressOrdering]] for more info.
   */
  def defaultAddressOrdering(order: Ordering[Address]): Unit = {
    addressOrdering = order
  }

  /**
   * Returns the default process global [[Address]] ordering as set via
   * `defaultAddressOrdering`. If no value is set, [[Address.HashOrdering]]
   * is used with the assumption that hosts resolved via Finagle provide the
   * load balancer with resolved InetAddresses. If a separate resolution process
   * is used, outside of Finagle, the default ordering should be overridden.
   */
  def defaultAddressOrdering: Ordering[Address] = addressOrdering

  /**
   * A reference to the current [[LoadBalancerFactory]] used by the stack
   * params in all the Finagle clients within this process.
   */
  @volatile private[this] var lbf: LoadBalancerFactory = FlagBalancerFactory

  /**
   * Set the default [[LoadBalancerFactory]] for the entire process (outside of
   * clients which override it).
   *
   * @see [[LoadBalancerFactory.Param]] for more info.
   */
  def defaultBalancerFactory(factory: LoadBalancerFactory): Unit = {
    lbf = factory
  }

  /**
   * Returns the default process global [[LoadBalancerFactory]] as set via
   * `defaultBalancerFactory`.
   */
  def defaultBalancerFactory: LoadBalancerFactory = lbf
}
