package com.twitter.finagle.loadbalancer.aperture

import com.twitter.app.Flaggable
import com.twitter.finagle.loadbalancer.aperture.EagerConnectionsType.{EagerConnectionsType}
import com.twitter.finagle.{CoreToggles, Stack}
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.loadbalancer.exp.apertureEagerConnections

/**
 * A case class eligible for configuring a finagle client such that the Aperture
 * load balancers eagerly establish connections with the resolved endpoints in
 * the aperture.
 *
 * @param mode indicator for eager connections. This feature can explicitly be disabled
 */
private[twitter] case class EagerConnections private (
  mode: EagerConnectionsType) {
  val isEnabled: () => Boolean = () => mode != EagerConnectionsType.Disable
  def enabled: Boolean = isEnabled()
  def withForceDtab: Boolean = mode == EagerConnectionsType.ForceWithDtab
}

private[twitter] object EagerConnectionsType extends Enumeration {
  type EagerConnectionsType = Value
  val Enable, ForceWithDtab, Disable = Value

  implicit val flaggableEagerConnectionsType: Flaggable[EagerConnectionsType] =
    new Flaggable[EagerConnectionsType] {

      def parse(s: String): EagerConnectionsType = {
        Seq(Enable, Disable, ForceWithDtab)
          .find {
            _.toString.equalsIgnoreCase(s)
          }
          .getOrElse(throw new IllegalArgumentException(s"Unknown EagerConnectionsType value $s"))
      }
    }
}

private[twitter] object EagerConnections {
  private val toggle = CoreToggles(apertureEagerConnections.name)

  // The flag value takes precedence over the toggle
  private val default: EagerConnections = {
    val flagVal = apertureEagerConnections.get
    flagVal match {
      case Some(x: EagerConnectionsType) => EagerConnections(x)
      case _ =>
        // we lazily grab the server id until the toggle is evaluated to ensure
        // server initialization has completed.
        val serverHashCode = ServerInfo().id.hashCode
        // protect against the toggle not being present
        if (toggle.isDefined && toggle(serverHashCode)) EagerConnections(enabled = true)
        else EagerConnections(enabled = false)
    }
  }

  implicit val param: Stack.Param[EagerConnections] = {
    Stack.Param(default)
  }

  /**
   * Creates this param enabling eager connections
   */
  def apply(): EagerConnections = this(true)

  /**
   * Explicitly configure EagerConnections
   */
  def apply(enabled: Boolean): EagerConnections = if (enabled)
    EagerConnections(EagerConnectionsType.Enable)
  else EagerConnections(EagerConnectionsType.Disable)
}
