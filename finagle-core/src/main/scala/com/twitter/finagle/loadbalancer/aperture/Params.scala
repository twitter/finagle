package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Stack
import com.twitter.finagle.loadbalancer.exp.apertureEagerConnections

/**
 * A case class eligible for configuring a finagle client such that the Aperture
 * load balancers eagerly establish connections with the resolved endpoints in
 * the aperture.
 *
 * @param enabled indicator for eager connections. This feature can explictly be disabled
 */
private[twitter] case class EagerConnections(enabled: Boolean)

private[twitter] object EagerConnections {
  implicit val param: Stack.Param[EagerConnections] =
    Stack.Param(EagerConnections(apertureEagerConnections()))

  /*
   * Creates this param enabling eager connections
   */
  def apply(): EagerConnections = EagerConnections(true)
}
