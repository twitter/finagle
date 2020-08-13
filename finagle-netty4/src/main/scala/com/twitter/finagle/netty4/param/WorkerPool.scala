package com.twitter.finagle.netty4.param

import com.twitter.finagle.netty4.param.WorkerPool.toEventLoopGroup
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.{numWorkers, useNativeEpoll}
import com.twitter.finagle.util.BlockingTimeTrackingThreadFactory
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory
import java.util.concurrent.{Executor, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

/**
 * A class eligible for configuring the [[io.netty.channel.EventLoopGroup]] used
 * to execute I/O work for finagle clients and servers. The default is global and shared
 * among clients and servers such that we can inline work on the I/O threads. Modifying
 * the default has performance and instrumentation implications and should only be
 * done so with care. If there is particular work you would like to schedule off
 * the I/O threads, consider scheduling that work on a separate thread pool
 * more granularly (e.g. [[com.twitter.util.FuturePool]] is a good tool for this).
 */
case class WorkerPool(eventLoopGroup: EventLoopGroup) {
  def this(executor: Executor, numWorkers: Int) =
    this(toEventLoopGroup(executor, numWorkers))

  def mk(): (WorkerPool, Stack.Param[WorkerPool]) =
    (this, WorkerPool.workerPoolParam)
}

object WorkerPool {

  private[this] val workerPoolSize = new AtomicInteger(0)

  // We hold onto the reference so the gauge doesn't get GC'd
  private[this] val workerGauge = FinagleStatsReceiver.addGauge("netty4", "worker_threads") {
    workerPoolSize.get
  }

  private def toEventLoopGroup(executor: Executor, numWorkers: Int): EventLoopGroup = {
    workerPoolSize.addAndGet(numWorkers)
    if (useNativeEpoll() && Epoll.isAvailable) mkEpollEventLoopGroup(numWorkers, executor)
    else mkNioEventLoopGroup(numWorkers, executor)
  }

  // This uses the netty DefaultThreadFactory to create thread pool threads. This factory creates
  // special FastThreadLocalThreads that netty has specific optimizations for.
  // Microbenchmarks put allocations via the netty allocator pools at ~8% faster on
  // FastThreadLocalThreads than normal ones.
  private def mkNettyThreadFactory(): ThreadFactory = {
    val prefix = "finagle/netty4"
    val threadGroup = new ThreadGroup(Thread.currentThread().getThreadGroup, prefix)
    new DefaultThreadFactory(
      /* poolName */ prefix,
      /* daemon */ true,
      /* priority */ Thread.NORM_PRIORITY,
      /* threadGroup */ threadGroup
    )
  }

  // Netty will create `numWorkers` children in the `EventLoopGroup`. Each `EventLoop` will
  // pin itself to a thread acquired from the `executor` and will multiplex over channels.
  // Thus, with this configuration, we should not acquire more than `numWorkers`
  // threads from the `executor`.
  implicit val workerPoolParam: Stack.Param[WorkerPool] = Stack.Param(
    new WorkerPool(
      Executors.newCachedThreadPool(new BlockingTimeTrackingThreadFactory(mkNettyThreadFactory())),
      numWorkers()
    )
  )

  private[netty4] def mkEpollEventLoopGroup(numWorkers: Int, executor: Executor): EventLoopGroup =
    new EpollEventLoopGroup(numWorkers, executor)

  private[netty4] def mkNioEventLoopGroup(numWorkers: Int, executor: Executor): EventLoopGroup =
    new NioEventLoopGroup(numWorkers, executor)

}
