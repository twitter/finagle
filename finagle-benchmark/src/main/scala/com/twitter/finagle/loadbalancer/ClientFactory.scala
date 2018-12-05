package com.twitter.finagle.loadbalancer

import com.twitter.finagle.stats.{Stat, StatsReceiver}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.Future

private trait Client extends Service[Unit, Unit]

private object ClientFactory {
  def apply(id: String, balancer: ServiceFactory[Unit, Unit], sr: StatsReceiver): Client =
    new Client {
      private[this] val latencyStat = sr.stat("latency")
      private[this] val underlying = balancer.toService

      def apply(req: Unit): Future[Unit] =
        Stat.timeFuture(latencyStat) { underlying(req) }

      override def toString = id
    }
}
