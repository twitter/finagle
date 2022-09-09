package com.twitter.finagle.loadbalancer

import com.twitter.finagle.ServiceFactoryProxy

/**
 * The base type of nodes over which load is balanced. [[NodeT]]s define the
 * load metric that is used. [[DistributorT]]'s will use these to decide
 * where to balance the next request.
 */
private trait NodeT[Req, Rep] extends ServiceFactoryProxy[Req, Rep] {

  /**
   * The current load, in units of the active metric.
   */
  def load: Double

  /**
   * The underlying service factory which this node proxies to.
   */
  def factory: EndpointFactory[Req, Rep]

  override def toString: String = f"Node(load=$load%1.3f, factory=$factory)"
}
