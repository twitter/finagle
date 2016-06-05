package com.twitter.finagle.loadbalancer

import com.twitter.finagle.ServiceFactory

/**
 * The base type of nodes over which load is balanced.
 * Nodes define the load metric that is used; distributors
 * like P2C will use these to decide where to balance
 * the next request.
 */
protected[loadbalancer] trait NodeT[Req, Rep] extends ServiceFactory[Req, Rep] {
  type This

  /**
   * The current load, in units of the active metric.
   */
  def load: Double

  /**
   * The number of pending requests to this node.
   */
  def pending: Int

  /**
   * A token is a random integer identifying the node.
   * It persists through node updates.
   */
  def token: Int

  /**
   * The underlying service factory.
   */
  def factory: ServiceFactory[Req, Rep]
}
