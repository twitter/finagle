package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.concurrent.ForkingScheduler
import com.twitter.util.Awaitable
import com.twitter.util.Future
import java.util.concurrent.Executor

private class DelegatingForkingScheduler(underlying: ForkingScheduler) extends ForkingScheduler {
  override def submit(r: Runnable) =
    underlying.submit(r)
  override def tryFork[T](f: => Future[T]): Future[Option[T]] =
    underlying.tryFork(f)
  override def fork[T](f: => Future[T]) =
    underlying.fork(f)
  override def fork[T](executor: Executor)(fut: => Future[T]) =
    underlying.fork(executor)(fut)
  override def withMaxSyncConcurrency(concurrency: Int, maxWaiters: Int) =
    underlying.withMaxSyncConcurrency(concurrency, maxWaiters)
  override def asExecutorService() =
    FiberScheduler.asExecutorService(this)
  override def flush(): Unit =
    underlying.flush()
  override def numDispatches: Long =
    underlying.numDispatches
  override def blocking[T](f: => T)(implicit perm: Awaitable.CanAwait): T =
    underlying.blocking(f)
  override def redirectFuturePools() =
    underlying.redirectFuturePools()

}
