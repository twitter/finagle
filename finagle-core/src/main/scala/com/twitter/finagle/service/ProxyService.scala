package com.twitter.finagle.service

/**
 * A service that simply proxies requests to an underlying service
 * that may not exist yet.
 */

import collection.mutable.ArrayBuffer

import com.twitter.util.{Future, Promise, Throw, Return}

import com.twitter.finagle.{Service, ServiceNotAvailableException}

class ProxyService[Req, Rep](underlyingFuture: Future[Service[Req, Rep]])
  extends Service[Req, Rep]
{
  private[this] val requestBuffer = new ArrayBuffer[(Req, Promise[Rep])]
  private[this] var underlying: Option[Service[Req, Rep]] = None

  underlyingFuture respond { r =>
    synchronized {
      r match {
        case Return(service) =>
          underlying = Some(service)
          requestBuffer foreach { case (request, promise) =>
            service(request) respond { promise() = _ }
          }

        case Throw(exc) =>
          underlying = Some(new Service[Req, Rep] {
            def apply(request: Req): Future[Rep] =
              Future.exception(new ServiceNotAvailableException)
            override def release() = ()
            override def isAvailable = false
          })
      }
    }
  }

  def apply(request: Req): Future[Rep] = {
    // Double check -- just an atomic ref or @volatile ref.  no lock in most cases.
    synchronized {
      if (!underlying.isDefined) {
        val promise = new Promise[Rep]
        requestBuffer += ((request, promise))
        return promise
      }
    }

    underlying.get(request)
  }
}
