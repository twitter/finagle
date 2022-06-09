package com.twitter.finagle.netty4

import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.util.BlockingTimeTrackingThreadFactory
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

/**
 * Utilities to create and reuse Netty 4 worker threads.
 */
private object WorkerEventLoop {

  private[this] val workerPoolSize = new AtomicInteger(0)

  // We hold onto the reference so the gauge doesn't get GC'd
  private[this] val workerGauge = FinagleStatsReceiver.addGauge("netty4", "worker_threads") {
    workerPoolSize.get
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

  def make(executor: Executor, numWorkers: Int): EventLoopGroup = {
    workerPoolSize.addAndGet(numWorkers)
    if (useNativeEpoll() && Epoll.isAvailable) new EpollEventLoopGroup(numWorkers, executor)
    else new NioEventLoopGroup(numWorkers, executor)
  }

  lazy val Global: EventLoopGroup = make(
    Executors.newCachedThreadPool(new BlockingTimeTrackingThreadFactory(mkNettyThreadFactory())),
    numWorkers()
  )
}
