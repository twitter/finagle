package com.twitter.finagle.service

/**
 * A service that simply proxies requests to an underlying service
 * yielded through a Future.
 */

import collection.mutable.ArrayBuffer

import com.twitter.util.{Future, Promise, Throw, Return}
import com.twitter.finagle.{Service, ServiceNotAvailableException}

class ProxyService[Req, Rep](underlyingFuture: Future[Service[Req, Rep]])
  extends Service[Req, Rep]
{
  @volatile private[this] var proxy: Service[Req, Rep] =
    new Service[Req, Rep] {
      private[this] val requestBuffer = new ArrayBuffer[(Req, Promise[Rep])]
      private[this] var underlying: Option[Service[Req, Rep]] = None
      private[this] var didRelease = false

      underlyingFuture respond { r =>
        synchronized {
          r match {
            case Return(service) =>
              requestBuffer foreach { case (request, promise) =>
                service(request) respond { promise() = _ }
              }

              underlying = Some(service)

              if (didRelease) service.release()

            case Throw(exc) =>
              requestBuffer foreach { case (_, promise) => promise() = Throw(exc) }
              underlying = Some(new FailedService(new ServiceNotAvailableException))
          }
        }
      }

      def apply(request: Req): Future[Rep] = synchronized {
        underlying match {
          case Some(service) => service(request)
          case None =>
            val promise = new Promise[Rep]
            requestBuffer += ((request, promise))
            promise
        }
      }

      override def isAvailable = false
      override def release() = synchronized { didRelease = true }
    }

  underlyingFuture respond {
    case Return(service) =>
      proxy = service

    case Throw(exc) =>
      proxy = new FailedService(new ServiceNotAvailableException)
  }

  def apply(request: Req): Future[Rep] = proxy(request)
  override def release() = proxy.release()
  override def isAvailable = proxy.isAvailable
}
