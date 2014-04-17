package com.twitter.finagle.factory

import com.twitter.finagle.{ClientConnection, param, ServiceFactory, ServiceFactoryProxy, Stack, Stackable}
import com.twitter.finagle.util.Throwables
import com.twitter.finagle.stats.StatsReceiver

private[finagle] object StatsFactoryWrapper {
  object ServiceCreationStats extends Stack.Role

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.StatsFactoryWrapper]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](ServiceCreationStats) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = params[param.Stats]
        new StatsFactoryWrapper(next, statsReceiver.scope("service_creation"))
      }
    }
}

/**
 * A [[com.twitter.finagle.ServiceFactoryProxy]] that tracks statistics on
 * [[com.twitter.finagle.Service]] creation failures.
 */
class StatsFactoryWrapper[Req, Rep](
    self: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver)
  extends ServiceFactoryProxy[Req, Rep](self)
{
  private[this] val failureStats = statsReceiver.scope("failures")

  override def apply(conn: ClientConnection) = super.apply(conn) onFailure { e =>
    failureStats.counter(Throwables.mkString(e): _*).incr()
  }
}
