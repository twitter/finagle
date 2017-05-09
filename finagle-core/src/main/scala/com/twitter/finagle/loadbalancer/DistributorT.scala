package com.twitter.finagle.loadbalancer

/**
 * The base type of the load balancer distributor. Distributors are
 * updated nondestructively, but, as with nodes, may share some
 * data across updates.
 *
 * @param vector the vector of nodes over which the balancer is balancing.
 */
private abstract class DistributorT[Node](val vector: Vector[Node]) {

  type This <: DistributorT[Node]

  /**
   * Pick the next node.
   *
   * This is the main entry point for a load balancer implementation.
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
