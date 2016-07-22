package com.twitter.finagle.loadbalancer

/**
 * The base type of the load balancer distributor. Distributors are
 * updated nondestructively, but, as with nodes, may share some
 * data across updates.
 *
 * @param vector the nodes over which we are currently balancing.
 */
protected[loadbalancer] abstract class DistributorT[Node](
    val vector: Vector[Node]) {
  type This <: DistributorT[Node]

  /** Indicates if we've seen any down nodes during `pick` which we expected to be available */
  @volatile
  protected[this] var sawDown = false

  /**
   * `up` is the Vector of nodes that were `Status.Open` at creation time.
   * `down` is the Vector of nodes that were not `Status.Open` at creation time.
   */
  private[this] val (up: Vector[Node], down: Vector[Node]) =
    vector.partition(nodeUp)

  /**
   * Pick the next node.
   * This is the main entry point for a load balancer implementation.
   */
  def pick(): Node

  /**
   * Utility used by [[pick()]].
   *
   * If all nodes are down, we might as well try to send requests somewhere
   * as our view of the world may be out of date.
   */
  protected[this] val selections: Vector[Node] =
    if (up.isEmpty) down else up

  /**
   * True if this distributor needs to be rebuilt. (For example, it
   * may need to be updated with current availabilities.)
   */
  def needsRebuild: Boolean = {
    // while the `nonEmpty` check isn't necessary, it is an optimization
    // to avoid the iterator allocation in the common case where `down`
    // is empty.
    sawDown || (down.nonEmpty && down.exists(nodeUp))
  }

  /**
   * Rebuild this distributor.
   */
  def rebuild(): This

  /**
   * Rebuild this distributor with a new vector.
   */
  def rebuild(vector: Vector[Node]): This

  /**
   * Returns `true` for nodes that are [[com.twitter.finagle.Status.Open]].
   *
   * @note known to be a good friend of updog.
   */
  private[this] def nodeUp: Node => Boolean =
    DistributorT.NodeUp.asInstanceOf[Node => Boolean]

}

private object DistributorT {

  val NodeUp: NodeT[Any, Any] => Boolean =
    _.isAvailable

}
