package com.twitter.finagle.client

import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.service.DelayedFactory
import com.twitter.finagle.{Addr, Name, Stack, Stackable, ServiceFactory}
import com.twitter.util.Future

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
      val description = "May extract metadata from the destination address and name"
      val parameters = Seq(
        implicitly[Stack.Param[LoadBalancerFactory.Dest]],
        implicitly[Stack.Param[BindingFactory.Dest]]
      )

      def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val LoadBalancerFactory.Dest(addr) = params[LoadBalancerFactory.Dest]
        val BindingFactory.Dest(name) = params[BindingFactory.Dest]

        val idMetadata = name match {
          case bound: Name.Bound => Addr.Metadata("id" -> bound.idStr)
          case _ => Addr.Metadata.empty
        }

        // delay construction of the ServiceFactory while Addr is Pending
        val futureFactory: Future[ServiceFactory[Req, Rep]] =
          addr.changes.collect {
            case Addr.Failed(_) | Addr.Neg =>
              next.make(params + AddrMetadata(idMetadata))

            case Addr.Bound(_, metadata) =>
              next.make(params + AddrMetadata(metadata ++ idMetadata))
          }.toFuture

        val delayed = DelayedFactory.swapOnComplete(futureFactory)
        Stack.leaf(this, delayed)
      }
    }
}
