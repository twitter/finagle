package com.twitter.finagle.service

import com.twitter.finagle.Service
import com.twitter.finagle.util.AsyncLatch

private[finagle] class RefcountedService[Req, Rep](underlying: Service[Req, Rep])
  extends Service[Req, Rep]
{
  protected[this] val replyLatch = new AsyncLatch

  def apply(request: Req) = {
    replyLatch.incr()
    underlying(request) ensure { replyLatch.decr() }
  }

  override def isAvailable = underlying.isAvailable
  override final def release() = replyLatch.await { doRelease() }

  protected def doRelease() = underlying.release()
}
