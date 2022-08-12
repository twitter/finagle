package com.twitter.finagle.balancersim

import com.twitter.finagle.stats.Stat
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
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
