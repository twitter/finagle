package com.twitter.finagle.loadbalancer.aperture

import com.twitter.app.Flaggable
import com.twitter.finagle.loadbalancer.aperture.EagerConnectionsType.{EagerConnectionsType}
import com.twitter.finagle.Stack
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
  def enabled: Boolean = mode != EagerConnectionsType.Disable
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

  // the default Stack.Param value takes into account the flag value
  private[this] val default: EagerConnections = EagerConnections(apertureEagerConnections())

  implicit val param: Stack.Param[EagerConnections] =
    Stack.Param(default)

  /**
   * Creates this param enabling eager connections
   */
  def apply(): EagerConnections = apply(true)

  /**
   * Explicitly configure EagerConnections
   */
  def apply(enabled: Boolean): EagerConnections = if (enabled)
    EagerConnections(EagerConnectionsType.Enable)
  else EagerConnections(EagerConnectionsType.Disable)
}
