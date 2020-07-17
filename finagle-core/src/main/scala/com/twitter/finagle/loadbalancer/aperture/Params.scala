package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.{CoreToggles, Stack}
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.loadbalancer.exp.apertureEagerConnections

/**
 * A case class eligible for configuring a finagle client such that the Aperture
 * load balancers eagerly establish connections with the resolved endpoints in
 * the aperture.
 *
 * @param isEnabled indicator for eager connections. This feature can explicitly be disabled
 */
private[twitter] case class EagerConnections private (isEnabled: () => Boolean) {
  def enabled: Boolean = isEnabled()
}

private[twitter] object EagerConnections {
  private val toggle = CoreToggles(apertureEagerConnections.name)

  // re-evaluate the default on each call for "kill-switch" functionality via the toggle.
  // The flag value takes precedence over the toggle
  private val default: () => Boolean = () => {
    apertureEagerConnections.get match {
      case Some(eagerConnections) => eagerConnections
      case None =>
        // we lazily grab the server id until the toggle is evaluated to ensure
        // server initialization has completed.
        val serverHashCode = ServerInfo().id.hashCode
        // protect against the toggle not being present
        toggle.isDefined && toggle(serverHashCode)
    }
  }

  implicit val param: Stack.Param[EagerConnections] =
    Stack.Param(EagerConnections(default))

  /**
   * Creates this param enabling eager connections
   */
  def apply(): EagerConnections = this(true)

  /**
   * Explicitly configure EagerConnections
   */
  def apply(enabled: Boolean): EagerConnections = EagerConnections(() => enabled)
}
