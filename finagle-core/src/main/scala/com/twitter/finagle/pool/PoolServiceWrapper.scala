package com.twitter.finagle.pool

import com.twitter.finagle.Service
import com.twitter.finagle.util.FutureLatch

private[pool] abstract class PoolServiceWrapper[Req, Rep](underlying: Service[Req, Rep])
  extends Service[Req, Rep]
{
  private[this] val replyLatch = new FutureLatch

  def apply(request: Req) = {
    replyLatch.incr()
    underlying(request) ensure { replyLatch.decr() }
  }

  override def isAvailable = underlying.isAvailable
  override final def release() = replyLatch.await { doRelease }

  protected def doRelease()
}
