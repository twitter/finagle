package com.twitter.finagle.factory

import com.twitter.finagle.{ServiceFactory, ServiceFactoryProxy}
import com.twitter.finagle.service.RefcountedService

private[finagle] class RefcountedFactory[Req, Rep](
  self: ServiceFactory[Req, Rep]
) extends ServiceFactoryProxy(self) {
  override def make() = super.make() map { new RefcountedService(_) }
}