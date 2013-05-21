package com.twitter.finagle.service

import com.twitter.finagle.util.AsyncLatch
import com.twitter.finagle.{Service, ServiceFactory, ClientConnection}
import com.twitter.util.{Future, Promise, Time}

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
      p.setValue(())
    }
    p
  }

  override def isAvailable = service.isAvailable

  override val toString = "singleton_factory_%s".format(service.toString)
}
