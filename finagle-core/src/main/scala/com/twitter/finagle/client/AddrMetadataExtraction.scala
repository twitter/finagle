package com.twitter.finagle.client

import com.twitter.finagle.loadbalancer.LoadBalancerFactory.Dest
import com.twitter.finagle.service.DelayedFactory
import com.twitter.finagle.{Addr, Stack, Stackable, ServiceFactory}
import com.twitter.util.{Future, Return, Try}

/**
 * Extraction of [[com.twitter.finagle.Addr.Metadata]] from a
 * [[com.twitter.finagle.factory.LoadBalancerFactory.Dest]]
 */
object AddrMetadataExtraction {
  object Role extends Stack.Role("AddrMetadataExtraction")

  /**
   * A class that may be configured by a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.client.AddrMetadataExtraction]] with bound address
   * metadata.
   */
  case class AddrMetadata(metadata: Addr.Metadata) {
    def mk(): (AddrMetadata, Stack.Param[AddrMetadata]) = (this, AddrMetadata.param)
  }
  object AddrMetadata {
    implicit val param = Stack.Param(AddrMetadata(Addr.Metadata.empty))
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = Role
      val description = "May extract metadata from the destination address"
      val parameters = Seq(implicitly[Stack.Param[Dest]])

      def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val Dest(addr) = params[Dest]

        // delay construction of the ServiceFactory while Addr is Pending
        val futureFactory: Future[ServiceFactory[Req, Rep]] =
          addr.changes.collect {
            case Addr.Failed(_) | Addr.Neg =>
              next.make(params)

            case Addr.Bound(_, metadata) =>
              next.make(params + AddrMetadata(metadata))
          }.toFuture

        val delayed = DelayedFactory.swapOnComplete(futureFactory)
        Stack.Leaf(this, delayed)
      }
    }
}