package com.twitter.finagle.factory

/**
 * The FactoryFailureStats maintains failure statistics on factory
 * creation.
 */

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stats.StatsReceiver

class StatsFactoryWrapper[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver)
  extends ServiceFactory[Req, Rep]
{
  private[this] val failureStats = statsReceiver.scope("failures")

  def make() = underlying.make() onFailure { e =>
    failureStats.counter(e.getClass.getName).incr()
  }

  override def close() = underlying.close()
  override def isAvailable = underlying.isAvailable
}
