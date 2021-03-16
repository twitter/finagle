package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.loadbalancer.NodeT
import com.twitter.finagle.util.Rng

/**
 * A NodeT which also has an associated random integer (token).
 * This token persists through node updates and allows for load balancing
 * with stable ordering across distributor rebuilds.
 */
private[aperture] trait ApertureNode[Req, Rep] extends NodeT[Req, Rep] {

  def tokenRng: Rng

  /**
   * A token is a random integer associated with an Aperture node.
   * It persists through node updates, but is not necessarily
   * unique. Aperture uses this token to order the nodes when
   * deterministic ordering is not enabled or available. Since
   * the token is assigned at Node creation, this guarantees
   * a stable order across distributor rebuilds.
   */
  val token: Int = tokenRng.nextInt()
}
