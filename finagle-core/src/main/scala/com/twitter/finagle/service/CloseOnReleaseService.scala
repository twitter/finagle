package com.twitter.finagle.service

import com.twitter.finagle.{Service, ServiceClosedException, ServiceProxy, WriteException}
import com.twitter.util.{Future, Time}
import java.util.concurrent.atomic.AtomicBoolean

private[finagle] class CloseOnReleaseService[Req, Rep](underlying: Service[Req, Rep])
  extends ServiceProxy[Req, Rep](underlying)
{
  private[this] val wasReleased = new AtomicBoolean(false)

  override def apply(request: Req) = {
    if (!wasReleased.get) {
      super.apply(request)
    } else {
      Future.exception(
        WriteException(new ServiceClosedException))
    }
  }

  override def close(deadline: Time) = {
    if (wasReleased.compareAndSet(false, true))
      super.close(deadline)
    else
      Future.Done
  }

  override def isAvailable = !wasReleased.get && super.isAvailable
}
