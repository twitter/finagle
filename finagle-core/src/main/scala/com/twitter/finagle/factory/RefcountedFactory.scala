package com.twitter.finagle.factory

import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy, ClientConnection}
import com.twitter.finagle.service.RefcountedService

/**
 * A [[com.twitter.finagle.ServiceFactoryProxy]] that wraps all
 * [[com.twitter.finagle.Service Services]] created by the argument
 * [[com.twitter.finagle.ServiceFactory]] in
 * [[com.twitter.finagle.RefcountedService RefcountedServices]].
 *
 * In essence, this factory is used to guarantee that services delay closure
 * until all outstanding requests have been completed.
 */
private[finagle] class RefcountedFactory[Req, Rep](
  self: ServiceFactory[Req, Rep]
) extends ServiceFactoryProxy(self) {
  override def apply(conn: ClientConnection) =
    super.apply(conn) map { new RefcountedService(_) }
}
