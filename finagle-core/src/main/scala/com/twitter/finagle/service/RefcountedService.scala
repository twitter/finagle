package com.twitter.finagle.service

import com.twitter.finagle.{Service, ServiceProxy}
import com.twitter.finagle.util.AsyncLatch

private[finagle] class RefcountedService[Req, Rep](underlying: Service[Req, Rep])
  extends ServiceProxy[Req, Rep](underlying)
{
  protected[this] val replyLatch = new AsyncLatch

  override def apply(request: Req) = {
    replyLatch.incr()
    underlying(request) ensure { replyLatch.decr() }
  }

  override final def release() = replyLatch.await { doRelease() }

  protected def doRelease() = underlying.release()
}
