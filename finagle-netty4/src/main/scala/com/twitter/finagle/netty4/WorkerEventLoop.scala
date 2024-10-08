package com.twitter.finagle.netty4

import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.util.BlockingTimeTrackingThreadFactory
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory
import io.netty.util.concurrent.SingleThreadEventExecutor
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

/**
 * Utilities to create and reuse Netty 4 worker threads.
 */
private object WorkerEventLoop {

  private[this] val workerPoolSize = new AtomicInteger(0)

  private[this] val eventLoopGroups = ConcurrentHashMap.newKeySet[EventLoopGroup]()

  // We hold onto the reference so the gauge doesn't get GC'd
  private[this] val workerGauge = FinagleStatsReceiver.addGauge("netty4", "worker_threads") {
    workerPoolSize.get
  }

  // We hold onto the reference so the gauge doesn't get GC'd
  private[this] val pendingEventsGauge =
    FinagleStatsReceiver.addGauge("netty4", "pending_io_events") {
      var total = 0l
      eventLoopGroups.forEach { group =>
        if (group.isShutdown) {
          eventLoopGroups.remove(group)
        } else {
          group.forEach {
            case loop: SingleThreadEventExecutor =>
              total += loop.pendingTasks()
            case _ => // nop
          }
        }
      }
      total
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

  def make(executor: Executor, numWorkers: Int, ioRatio: Int = 50): EventLoopGroup = {
    workerPoolSize.addAndGet(numWorkers)
    val result =
      if (useNativeEpoll() && Epoll.isAvailable) {
        val group = new EpollEventLoopGroup(numWorkers, executor)
        group.setIoRatio(ioRatio)
        group
      } else {
        new NioEventLoopGroup(numWorkers, executor)
      }

    eventLoopGroups.add(result)

    result
  }

  lazy val Global: EventLoopGroup = make(
    executor = Executors.newCachedThreadPool(
      new BlockingTimeTrackingThreadFactory(mkNettyThreadFactory())
    ),
    numWorkers = numWorkers(),
    ioRatio = ioRatio()
  )
}
