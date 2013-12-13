package com.twitter.finagle.service

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory}
import com.twitter.util.{Await, Future, Promise, Time}

/**
 * A factory that won't satisfy the service future until an underlying service factory
 * is ready.
 *
 * Close closes the underlying service factory, which means that it won't be
 * satisfied until after the underlying future has been satisfied.
 *
 * @param underlyingF The future should be satisfied when the underlying factory is ready
 */
class DelayedFactory[Req, Rep](
  underlyingF: Future[ServiceFactory[Req, Rep]]
) extends ServiceFactory[Req, Rep] {
  def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    underlyingF.flatMap(_(conn))

  override def close(deadline: Time) = underlyingF flatMap { _.close(deadline) }
  override def isAvailable = underlyingF.isDefined && Await.result(underlyingF).isAvailable
}
