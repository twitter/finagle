package com.twitter.finagle.factory

import com.twitter.util.{Future, Duration, Timer}

import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy, ServiceTimeoutException, ClientConnection}

/**
 * A factory wrapper that times out the service acquisition after the
 * given time.
 */
class TimeoutFactory[Req, Rep](
    self: ServiceFactory[Req, Rep],
    timeout: Duration,
    exception: ServiceTimeoutException,
    timer: Timer)
  extends ServiceFactoryProxy[Req, Rep](self)
{
  override def apply(conn: ClientConnection) = {
    val res = super.apply(conn)
    res.within(timer, timeout) rescue {
      case exc: java.util.concurrent.TimeoutException =>
        res.raise(exc)
        res onSuccess { _.close() }
        Future.exception(exception)
    }
  }
}
