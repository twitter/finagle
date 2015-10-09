package com.twitter.finagle.service

import com.twitter.finagle.{Status, Service, ServiceClosedException, ServiceProxy, WriteException}
import com.twitter.util.{Future, Time}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [[com.twitter.finagle.Service]] that rejects all requests after its `close`
 * method has been invoked.
 */
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

  override def status = 
    if (wasReleased.get) Status.Closed
    else super.status
}
