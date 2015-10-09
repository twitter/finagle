package com.twitter.finagle.client

import com.twitter.finagle.client.AddrMetadataExtraction.AddrMetadata
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{Addr, Stack, Stackable, ServiceFactory}
import com.twitter.util.Duration

/**
 * Stats scoping enabled the modification of the StatsReceiver scoping on a
 * per-endpoint basis.  For instance, if a client has endpoints in multiple
 * zones, a scoper might add a per-zone scope.
 */
object StatsScoping {
  object Role extends Stack.Role("StatsScoping")

  type ScoperFunction = (StatsReceiver, Addr.Metadata) => StatsReceiver

  /**
   * A Scoper is a function that takes an existing StatsReceiver and an
   * arbitrary address metadata map and computes a new StatsReceiver.
   */
  case class Scoper(scoper: ScoperFunction) {
    def mk(): (Scoper, Stack.Param[Scoper]) = (this, Scoper.param)
  }
  object Scoper {
    implicit val param = Stack.Param(Scoper { (stats, _) => stats })
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = Role
      val description = "May modify stats scoping based on the destination address"
      val parameters = Seq(
        implicitly[Stack.Param[AddrMetadata]],
        implicitly[Stack.Param[Scoper]],
        implicitly[Stack.Param[Stats]])

      def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val AddrMetadata(metadata) = params[AddrMetadata]
        val Stats(stats) = params[Stats]
        val Scoper(scoper) = params[Scoper]

        val scoped = scoper(stats, metadata)
        val stack = next.make(params + Stats(scoped))
        Stack.Leaf(this, stack)
      }
    }
}
