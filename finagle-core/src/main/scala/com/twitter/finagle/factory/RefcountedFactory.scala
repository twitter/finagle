package com.twitter.finagle.factory

import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy, ClientConnection}
import com.twitter.finagle.service.RefcountedService

private[finagle] class RefcountedFactory[Req, Rep](
  self: ServiceFactory[Req, Rep]
) extends ServiceFactoryProxy(self) {
  override def apply(conn: ClientConnection) =
    super.apply(conn) map { new RefcountedService(_) }
}
