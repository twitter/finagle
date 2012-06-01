package com.twitter.finagle.service

import collection.mutable.Queue
import com.twitter.finagle.{CancelledConnectionException, TooManyConcurrentRequestsException, Service, ServiceNotAvailableException}
import com.twitter.util.{Future, Promise, Throw, Return}

/**
 * A service that simply proxies requests to an underlying service
 * yielded through a Future.
 */
class ProxyService[Req, Rep](underlyingFuture: Future[Service[Req, Rep]], maxWaiters: Int = Int.MaxValue)
  extends Service[Req, Rep]
{
  @volatile private[this] var proxy: Service[Req, Rep] =
    new Service[Req, Rep] {
      private[this] val requestBuffer = new Queue[(Req, Promise[Rep])]
      private[this] var underlying: Option[Service[Req, Rep]] = None
      private[this] var didRelease = false

      underlyingFuture respond { r =>
        synchronized {
          r match {
            case Return(service) =>
              requestBuffer foreach { case (request, promise) =>
                val res = service(request) respond { promise() = _ }
                promise.linkTo(res)
              }

              underlying = Some(service)

              if (didRelease) service.release()

            case Throw(exc) =>
              requestBuffer foreach { case (_, promise) => promise() = Throw(exc) }
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
              val promise = new Promise[Rep]
              requestBuffer += ((request, promise))
              promise onCancellation {
                val dq = synchronized { requestBuffer.dequeueFirst { case (_, p) => p eq promise } }
                dq foreach { case (_, p) => p() = Throw(new CancelledConnectionException)}
              }
              promise
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
