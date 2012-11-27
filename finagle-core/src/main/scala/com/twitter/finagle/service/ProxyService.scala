package com.twitter.finagle.service

import com.twitter.finagle.{CancelledConnectionException, Service, ServiceNotAvailableException,
  TooManyConcurrentRequestsException}
import com.twitter.util.{Future, Promise, Throw, Return}
import java.util.ArrayDeque
import scala.collection.JavaConverters._

/**
 * A service that simply proxies requests to an underlying service
 * yielded through a Future.
 */
class ProxyService[Req, Rep](underlyingFuture: Future[Service[Req, Rep]], maxWaiters: Int = Int.MaxValue)
  extends Service[Req, Rep]
{
  @volatile private[this] var proxy: Service[Req, Rep] =
    new Service[Req, Rep] {
      private[this] val requestBuffer = new ArrayDeque[(Req, Promise[Rep])]
      private[this] var underlying: Option[Service[Req, Rep]] = None
      private[this] var didRelease = false

      underlyingFuture respond { r =>
        synchronized {
          r match {
            case Return(service) =>
              requestBuffer.asScala foreach { case (request, promise) =>
                promise.become(service(request))
              }

              underlying = Some(service)

              if (didRelease) service.release()

            case Throw(exc) =>
              requestBuffer.asScala foreach { case (_, promise) => promise() = Throw(exc) }
              underlying = Some(new FailedService(new ServiceNotAvailableException))
          }
          requestBuffer.clear()
        }
      }

      def apply(request: Req): Future[Rep] = synchronized {
        underlying match {
          case Some(service) => service(request)
          case None =>
            if (requestBuffer.size >= maxWaiters)
              Future.exception(new TooManyConcurrentRequestsException)
            else {
              val p = new Promise[Rep]
              val waiting = (request, p)
              requestBuffer.addLast(waiting)
              p.setInterruptHandler { case cause =>
                if (ProxyService.this.synchronized(requestBuffer.remove(waiting)))
                  p.setException(new CancelledConnectionException)
              }

              p
            }
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
