package com.twitter.finagle

import com.twitter.util.{Future, Time}

/**
 * Turns a [[com.twitter.finagle.ServiceFactory]] into a
 * [[com.twitter.finagle.Service]] which acquires a new service for
 * each request.
 */
class FactoryToService[Req, Rep](factory: ServiceFactory[Req, Rep]) extends Service[Req, Rep] {
  def apply(request: Req): Future[Rep] =
    factory().flatMap { service =>
      service(request).ensure {
        service.close()
      }
    }

  override def close(deadline: Time): Future[Unit] = factory.close(deadline)
  override def status: Status = factory.status
}

object FactoryToService {
  val role = Stack.Role("FactoryToService")

  // TODO: we should simply transform the stack for boolean
  // stackables like this.
  case class Enabled(enabled: Boolean) {
    def mk(): (Enabled, Stack.Param[Enabled]) =
      (this, Enabled.param)
  }
  object Enabled {
    implicit val param = Stack.Param(Enabled(false))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[FactoryToService]]. This makes per-request service acquisition
   * part of the stack so it can be wrapped by filters such as tracing.
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[Enabled, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = FactoryToService.role
      val description = "Apply service factory on each service request"
      def make(_enabled: Enabled, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        if (_enabled.enabled) {
          /*
           * The idea here is to push FactoryToService down the stack
           * so that service acquisition in the course of a request is
           * wrapped by tracing, timeouts, etc. for that request.
           *
           * We can't return a Service directly (since the stack type
           * is ServiceFactory); instead we wrap it in a
           * ServiceFactoryProxy which returns the singleton Service.
           * An outer FactoryToService (wrapping the whole stack)
           * unwraps it.
           *
           * This outer FactoryToService also closes the service after
           * each request, but we don't want to close the singleton,
           * since it is itself a FactoryToService, so closing it
           * closes the underlying factory; thus we wrap the service
           * in a proxy which ignores the close.
           *
           * The underlying services are still closed by the inner
           * FactoryToService, and the underlying factory is still
           * closed when close is called on the outer FactoryToService.
           *
           * This is too complicated.
           */
          val service = Future.value(new ServiceProxy[Req, Rep](new FactoryToService(next)) {
            override def close(deadline: Time): Future[Unit] = Future.Done
          })
          new ServiceFactoryProxy(next) {
            override def apply(conn: ClientConnection): Future[ServiceProxy[Req, Rep]] = service
          }
        } else {
          next
        }
      }
    }
}
