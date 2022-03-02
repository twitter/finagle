package com.twitter.finagle.loadbalancer

/**
 * Information about a [[Balancer load balancer]].
 *
 * This class is thread-safe and while the class itself is immutable,
 * it proxies data from a [[Balancer]] which may be mutable.
 *
 * @param label the [[com.twitter.finagle.param.Label]] used to identify the client
 *              using this balancer.
 * @param balancer the load balancer being used.
 *
 * @see [[BalancerRegistry]]
 * @see TwitterServer's "/admin/balancers.json" admin endpoint.
 */
final class Metadata private[loadbalancer] (val label: String, balancer: Balancer[_, _]) {

  override def toString: String = s"${getClass.getName}($label)"

  /** Any additional metadata specific to a balancer implementation. */
  def additionalInfo: Map[String, Any] = balancer.additionalMetadata

  /** The class name of the balancer implementation. */
  def balancerClass: String =
    balancer.getClass.getSimpleName

  /** The status of the underlying balancer. */
  def status: String =
    balancer.status.toString

  /** How many nodes are available to receive traffic. E.g. not Busy or Closed */
  def numAvailable: Int =
    balancer.numAvailable

  /** How many nodes have a Busy status */
  def numBusy: Int =
    balancer.numBusy

  /** How many nodes have a Closed status */
  def numClosed: Int =
    balancer.numClosed

  /** The total number of pending requests across all nodes. */
  def totalPending: Int =
    balancer.totalPending

  /**
   * The total load across all nodes.
   *
   * Load is a balancer implementation specific construct.
   */
  def totalLoad: Double =
    balancer.totalLoad

  /** How many nodes are being balanced across */
  def size: Int =
    balancer.size

  /**
   * What percent of nodes (in the aperture) can be unhealthy
   * before panic mode is enabled for a request
   */
  def panicMode: String = balancer.panicMode.toString
}
