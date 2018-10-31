package com.twitter.finagle

import com.twitter.util.{Future, Time}

/**
 * A simple proxy Service that forwards all calls to another Service.
 * This is useful if you want to wrap-but-modify an existing service.
 */
abstract class ServiceProxy[-Req, +Rep](val self: Service[Req, Rep])
    extends Service[Req, Rep]
    with Proxy {
  def apply(request: Req): Future[Rep] = self(request)
  override def close(deadline: Time): Future[Unit] = self.close(deadline)

  override def status: Status = self.status

  override def toString: String = self.toString
}
