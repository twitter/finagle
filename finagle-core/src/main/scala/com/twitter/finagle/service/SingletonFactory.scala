package com.twitter.finagle.service

import com.twitter.finagle.util.AsyncLatch
import com.twitter.finagle.{Service, ServiceFactory, ClientConnection}
import com.twitter.util.{Future, Promise, Time}

/**
 * A [[com.twitter.finagle.ServiceFactory]] that produces
 * [[com.twitter.finagle.Service Services]] identical to the argument `service`.
 *
 * Note that this factory builds new [[com.twitter.finagle.Service Services]],
 * so the "singleton" `service` argument is not shared by reference. This
 * differs from [[com.twitter.finagle.ServiceFactory#const]] in that `const`
 * proxies all requests to the same `service` rather than creating new objects.
 */
class SingletonFactory[Req, Rep](service: Service[Req, Rep])
  extends ServiceFactory[Req, Rep]
{
  private[this] var latch = new AsyncLatch

  def apply(conn: ClientConnection) = Future {
    latch.incr()
    new Service[Req, Rep] {
      def apply(request: Req) = service(request)
      override def close(deadline: Time) = { latch.decr(); Future.Done }
    }
  }

  def close(deadline: Time) = {
    val p = new Promise[Unit]
    latch.await {
      service.close()
      p.setDone()
    }
    p
  }

  override def status = service.status

  override val toString = "singleton_factory_%s".format(service.toString)
}
