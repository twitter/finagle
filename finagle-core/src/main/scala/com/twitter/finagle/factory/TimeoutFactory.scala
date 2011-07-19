package com.twitter.finagle.factory

/**
 * A factory wrapper that times out the service acquisition after the
 * given time.
 */

import com.twitter.util
import com.twitter.util.{Future, Duration, TimeoutException}

import com.twitter.finagle.{
  ServiceFactory, ServiceFactoryProxy,
  ServiceTimeoutException}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Timer

class TimeoutFactory[Req, Rep](
    self: ServiceFactory[Req, Rep],
    timeout: Duration,
    timer: util.Timer = Timer.default)
  extends ServiceFactoryProxy[Req, Rep](self)
{
  override def make() = {
    val res = super.make()

    res.within(timer, timeout) rescue {
      case _: TimeoutException =>
        res.cancel()
        res onSuccess { _.release() }
        Future.exception(new ServiceTimeoutException)
    }
  }
}
