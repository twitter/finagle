package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.{CoreToggles, Stack}
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.loadbalancer.exp.apertureEagerConnections

/**
 * A case class eligible for configuring a finagle client such that the Aperture
 * load balancers eagerly establish connections with the resolved endpoints in
 * the aperture.
 *
 * @param enabled indicator for eager connections. This feature can explictly be disabled
 */
private[twitter] case class EagerConnections private (isEnabled: () => Boolean) {
  def enabled: Boolean = isEnabled()
}

private[twitter] object EagerConnections {
  private val toggle = CoreToggles(apertureEagerConnections.name)
  private val serverHashCode = ServerInfo().id.hashCode

  // re-evaluate the default on each call for "kill-switch" functionality via the toggle.
  // The flag value takes precedence over the toggle
  private val default: () => Boolean = () => {
    apertureEagerConnections.get match {
      case Some(eagerConnections) => eagerConnections
      case None =>
        // protect against the toggle not being present
        if (toggle.isDefinedAt(serverHashCode)) toggle(hashCode)
        else false
    }
  }

  implicit val param: Stack.Param[EagerConnections] =
    Stack.Param(EagerConnections(default))

  /**
   * Creates this param enabling eager connections
   */
  def apply(): EagerConnections = this(true)

  /**
   * Explictly configure EagerConnections
   */
  def apply(enabled: Boolean): EagerConnections = EagerConnections(() => enabled)
}
