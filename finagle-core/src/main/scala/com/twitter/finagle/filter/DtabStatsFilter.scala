package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver

object DtabStatsFilter {
  val role = Stack.Role("DtabStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.DtabStatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = DtabStatsFilter.role
      val description = "Report dtab statistics"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new DtabStatsFilter[Req, Rep](statsReceiver) andThen next
      }
    }
}

/**
 * Adds a Stat, dtab/local/size, that tracks the size of Dtab.local for all
 * requests with a non-empty Dtab.
 */
class DtabStatsFilter[Req, Rsp](statsReceiver: StatsReceiver)
    extends SimpleFilter[Req, Rsp] {

  private[this] val dtabSizes = statsReceiver.stat("dtab", "size")

  def apply(request: Req, service: Service[Req, Rsp]) = {
    if (Dtab.local.nonEmpty) {
      dtabSizes.add(Dtab.local.size)
    }
    service(request)
  }
}
