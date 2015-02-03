package com.twitter.finagle.client

import com.twitter.finagle.loadbalancer.LoadBalancerFactory.AddrMetadata
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.{Addr, Stack, Stackable, ServiceFactory}
import com.twitter.util.Duration

/**
 * Latency compensation enables the modification of connection and
 * request timeouts on a per-endpoint basis.  For instance, if a
 * client has both network-local and trans-continental endpoints, a
 * reasonable latency compensator might add the speed-of-light penalty
 * when communicating with distant endpoints.
 */
object LatencyCompensation {

  object Role extends Stack.Role("LatencyCompensation")

  /**
   * A compensator is a function that takes an arbitrary address metadata map and computes a
   * latency compensation that is added to connection and request timeouts.
   */
  case class Compensator(compensator: Addr.Metadata => Duration) {
    def mk(): (Compensator, Stack.Param[Compensator]) =
      (this, Compensator.param)
  }
  object Compensator {
    implicit val param =
      Stack.Param(Compensator(_ => Duration.Zero))
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = Role
      val description = "May modify timeouts based on the destination address"
      val parameters = Seq(
        implicitly[Stack.Param[AddrMetadata]],
        implicitly[Stack.Param[Compensator]],
        implicitly[Stack.Param[TimeoutFilter.Param]],
        implicitly[Stack.Param[Transporter.ConnectTimeout]])
      def make(prms: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val AddrMetadata(metadata) = prms[AddrMetadata]
        val Compensator(compensate) = prms[Compensator]
        val compensation = compensate(metadata)
        val Transporter.ConnectTimeout(connect) = prms[Transporter.ConnectTimeout]
        val TimeoutFilter.Param(request) = prms[TimeoutFilter.Param]
        val compensated = next.make(prms +
          Transporter.ConnectTimeout(connect + compensation) +
          TimeoutFilter.Param(request + compensation))
        Stack.Leaf(this, compensated)
      }
    }

}
