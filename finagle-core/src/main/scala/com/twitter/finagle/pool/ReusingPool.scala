package com.twitter.finagle.pool

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory,
  ServiceFactoryProxy, ServiceProxy}
import com.twitter.util.{Future, Promise, Return, Throw, Try}
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

/**
 * A pool that maintains at most one service from the underlying
 * ServiceFactory. A new Service is established whenever the service
 * factory fails or the current service is unavailable.
 */
class ReusingPool[Req, Rep](underlying: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req, Rep](underlying)
{
  private[this] val current: AtomicReference[Future[ServiceProxy[Req, Rep]]] =
    new AtomicReference(Future.exception(new Exception))

  private[this] def newService(conn: ClientConnection) =
    underlying(conn) map { service =>
      new ServiceProxy(service) {
        override def release() = () // No-op
      }
    }

  @tailrec
  override final def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    val f = current.get
    f.poll match {
      case Some(Return(service)) if service.isAvailable => f
      // Still waiting for connect.
      case None => f
      // Last connect failed.
      case Some(Throw(_)) =>
        if (!current.compareAndSet(f, Future.never)) apply(conn) else {
          current.set(newService(conn))
          current.get
        }
      // Service is dead. Give it back.
      case Some(Return(dead)) =>
        if (!current.compareAndSet(f, Future.never)) apply(conn) else {
          dead.self.release()
          current.set(newService(conn))
          current.get
        }
    }
  }
}
