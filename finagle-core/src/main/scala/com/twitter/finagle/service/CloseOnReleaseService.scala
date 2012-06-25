package com.twitter.finagle.service

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.util.Future

import com.twitter.finagle.{
  Service, ServiceClosedException, ServiceProxy, WriteException}

private[finagle] class CloseOnReleaseService[Req, Rep](underlying: Service[Req, Rep])
  extends ServiceProxy[Req, Rep](underlying)
{
  private[this] val wasReleased = new AtomicBoolean(false)

  override def apply(request: Req) = {
    if (!wasReleased.get) {
      super.apply(request)
    } else {
      Future.exception(
        new WriteException(new ServiceClosedException))
    }
  }

  override def release() {
    if (wasReleased.compareAndSet(false, true))
      super.release()
  }

  override def isAvailable = !wasReleased.get && super.isAvailable
}
