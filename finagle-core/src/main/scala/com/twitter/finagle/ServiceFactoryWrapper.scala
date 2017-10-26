package com.twitter.finagle

/**
 * A ServiceFactoryWrapper adds behavior to an underlying ServiceFactory.
 */
trait ServiceFactoryWrapper {
  def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep]
}

object ServiceFactoryWrapper {
  val identity: ServiceFactoryWrapper = new ServiceFactoryWrapper {
    def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = factory
  }
}
