package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.util.Future

object DtabStatsFilter {
  val role = Stack.Role("DtabStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.DtabStatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = DtabStatsFilter.role
      val description = "Report dtab statistics"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new DtabStatsFilter[Req, Rep](statsReceiver) andThen next
      }
    }
}

/**
 * Adds three stats:
 *    - dtab/size, a default stat that tracks the size of both Dtab.limited and Dtab.local for all
 *                  requests with a non-empty Dtab
 *    - dtab/local/size, a debug stat which tracks just the size of Dtab.local for all requests
 *                  with a non-empty Dtab
 *    - dtab/limited/size, a debug stat which tracks just the size of Dtab.limited for all requests
 *                  with a non-empty Dtab
 */
class DtabStatsFilter[Req, Rep](statsReceiver: StatsReceiver) extends SimpleFilter[Req, Rep] {

  private[this] val dtabSizes = statsReceiver.stat("dtab", "size")
  private[this] val dtabLocalSize = statsReceiver.stat(Verbosity.Debug, "dtab", "local", "size")
  private[this] val dtabLimitedSize = statsReceiver.stat(Verbosity.Debug, "dtab", "limited", "size")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val localSize =
      if (Dtab.local.nonEmpty) {
        val size = Dtab.local.size
        dtabLocalSize.add(size)
        size
      } else 0

    val limitedSize =
      if (Dtab.limited.nonEmpty) {
        val size = Dtab.limited.size
        dtabLimitedSize.add(size)
        size
      } else 0

    val totalSize = localSize + limitedSize
    if (totalSize > 0) dtabSizes.add(totalSize)
    service(request)
  }
}
