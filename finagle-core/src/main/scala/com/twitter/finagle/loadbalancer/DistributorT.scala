package com.twitter.finagle.loadbalancer

/**
 * The base type of the load balancer distributor. Distributors are
 * updated nondestructively, but, as with nodes, may share some
 * data across updates.
 */
protected[loadbalancer] trait DistributorT[Node] {
  type This

  /**
   * The vector of nodes over which we are currently balancing.
   */
  def vector: Vector[Node]

  /**
   * Pick the next node. This is the main load balancer.
   */
  def pick(): Node

  /**
   * True if this distributor needs to be rebuilt. (For example, it
   * may need to be updated with current availabilities.)
   */
  def needsRebuild: Boolean

  /**
   * Rebuild this distributor.
   */
  def rebuild(): This

  /**
   * Rebuild this distributor with a new vector.
   */
  def rebuild(vector: Vector[Node]): This
}
