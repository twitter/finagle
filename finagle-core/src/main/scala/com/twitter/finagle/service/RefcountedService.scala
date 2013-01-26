package com.twitter.finagle.service

import com.twitter.finagle.{Service, ServiceProxy}
import com.twitter.finagle.util.AsyncLatch
import com.twitter.util.{Future, Promise, Time}

private[finagle] class RefcountedService[Req, Rep](underlying: Service[Req, Rep])
  extends ServiceProxy[Req, Rep](underlying)
{
  protected[this] val replyLatch = new AsyncLatch

  override def apply(request: Req) = {
    replyLatch.incr()
    underlying(request) ensure { replyLatch.decr() }
  }

  override final def close(deadline: Time): Future[Unit] = {
    val p = new Promise[Unit]
    replyLatch.await {
      p.become(underlying.close(deadline))
    }
    p
  }
}
