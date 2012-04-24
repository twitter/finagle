package com.twitter.finagle.factory

/**
 * The FactoryFailureStats maintains failure statistics on factory
 * creation.
 */

import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy, ClientConnection}
import com.twitter.finagle.stats.StatsReceiver

class StatsFactoryWrapper[Req, Rep](
    self: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver)
  extends ServiceFactoryProxy[Req, Rep](self)
{
  private[this] val failureStats = statsReceiver.scope("failures")

  override def apply(conn: ClientConnection) = super.apply(conn) onFailure { e =>
    failureStats.counter(e.getClass.getName).incr()
  }
}
