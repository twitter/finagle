package com.twitter.finagle.service

import com.twitter.util.Future

import com.twitter.finagle.{Service, ServiceFactory}
       
class SingletonFactory[Req, Rep](service: Service[Req, Rep])
  extends ServiceFactory[Req, Rep]
{
  private[this] var outstandingInstances = 0
  private[this] var needsRelease = false

  def make() = synchronized {
    outstandingInstances += 1
    val wrapped = new Service[Req, Rep] {
      def apply(request: Req) = service(request)
      override def release() = SingletonFactory.this.synchronized {
        outstandingInstances -= 1
        if (outstandingInstances == 0 && needsRelease)
          service.release()
      }
    }

    Future.value(wrapped)
  }

  override def release() = synchronized {
    if (outstandingInstances == 0)
      service.release()
    else
      needsRelease = true
  }
}
