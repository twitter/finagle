package com.twitter.finagle.service

import com.twitter.util.Future

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.util.FutureLatch
       
class SingletonFactory[Req, Rep](service: Service[Req, Rep])
  extends ServiceFactory[Req, Rep]
{
  private[this] var latch = new FutureLatch

  def make() = {
    latch.incr()
    val wrapped = new Service[Req, Rep] {
      def apply(request: Req) = service(request)
      override def release() = latch.decr()
    }

    Future.value(wrapped)
  }

  def close() = latch.await { service.release() }
}
