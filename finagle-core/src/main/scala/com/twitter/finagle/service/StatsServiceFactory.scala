package com.twitter.finagle.service

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.ServiceFactoryProxy
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.param
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future
import com.twitter.util.Time

private[finagle] object StatsServiceFactory {

  private class StatsServiceFactory[Req, Rep](
    factory: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver)
      extends ServiceFactoryProxy[Req, Rep](factory) {

    private[this] val availableGauge = statsReceiver.addGauge("available") {
      if (isAvailable) 1f else 0f
    }

    override def close(deadline: Time): Future[Unit] = {
      availableGauge.remove()
      super.close(deadline)
    }
  }

  val role: Stack.Role = Stack.Role("FactoryStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.service.StatsServiceFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = StatsServiceFactory.role
      val description: String = "Report connection statistics"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new StatsServiceFactory(next, statsReceiver)
      }
    }
}
