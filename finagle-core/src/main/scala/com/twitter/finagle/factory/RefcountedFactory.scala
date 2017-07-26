package com.twitter.finagle.factory

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory, ServiceFactoryProxy}
import com.twitter.finagle.service.RefcountedService
import com.twitter.util.Future

/**
 * A [[com.twitter.finagle.ServiceFactoryProxy]] that wraps all
 * [[com.twitter.finagle.Service Services]] created by the argument
 * [[com.twitter.finagle.ServiceFactory]] in
 * [[com.twitter.finagle.service.RefcountedService RefcountedServices]].
 *
 * In essence, this factory is used to guarantee that services delay closure
 * until all outstanding requests have been completed.
 */
private[finagle] class RefcountedFactory[Req, Rep](underlying: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy(underlying) {

  private[this] val refCountedSvc: Service[Req, Rep] => Service[Req, Rep] =
    svc => new RefcountedService(svc)

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    super.apply(conn).map(refCountedSvc)

}
