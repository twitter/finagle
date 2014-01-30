package com.twitter.finagle.factory

import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy, ClientConnection}
import com.twitter.finagle.util.Throwables
import com.twitter.finagle.stats.StatsReceiver

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
