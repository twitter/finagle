package com.twitter.finagle.service

import com.twitter.finagle.{Service, ServiceProxy}
import com.twitter.finagle.util.AsyncLatch
import com.twitter.util.{Future, Promise, Time, Try}

/**
 * A [[com.twitter.finagle.Service]] that delays closure until all outstanding
 * requests have been completed.
 */
private[finagle] class RefcountedService[Req, Rep](underlying: Service[Req, Rep])
    extends ServiceProxy[Req, Rep](underlying) {

  private[this] val replyLatch = new AsyncLatch()

  private[this] val decrLatchFn: Try[Rep] => Unit =
    _ => replyLatch.decr()

  override def apply(request: Req): Future[Rep] = {
    replyLatch.incr()
    underlying(request).respond(decrLatchFn)
  }

  override final def close(deadline: Time): Future[Unit] = {
    val p = new Promise[Unit]
    replyLatch.await {
      p.become(underlying.close(deadline))
    }
    p
  }
}
