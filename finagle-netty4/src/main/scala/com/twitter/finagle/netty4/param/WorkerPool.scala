package com.twitter.finagle.netty4.param

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.{nativeEpoll, numWorkers}
import com.twitter.finagle.util.BlockingTimeTrackingThreadFactory
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.util.concurrent.{Executor, Executors}

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
  def this(executor: Executor, numWorkers: Int) = this(
    if (nativeEpoll.enabled) WorkerPool.mkEpollEventLoopGroup(numWorkers, executor)
    else WorkerPool.mkNioEventLoopGroup(numWorkers, executor))

  def mk() : (WorkerPool, Stack.Param[WorkerPool]) =
    (this, WorkerPool.workerPoolParam)
}

object WorkerPool {

  // Netty will create `numWorkers` children in the `EventLoopGroup`. Each `EventLoop` will
  // pin itself to a thread acquired from the `executor` and will multiplex over channels.
  // Thus, with this configuration, we should not acquire more than `numWorkers`
  // threads from the `executor`.
  implicit val workerPoolParam: Stack.Param[WorkerPool] = Stack.Param(
    new WorkerPool(Executors.newCachedThreadPool(new BlockingTimeTrackingThreadFactory(
      new NamedPoolThreadFactory("finagle/netty4", makeDaemons = true)
    )), numWorkers()))

  private[netty4] def mkEpollEventLoopGroup(numWorkers: Int, executor: Executor): EventLoopGroup =
    new EpollEventLoopGroup(numWorkers, executor)

  private[netty4] def mkNioEventLoopGroup(numWorkers: Int, executor: Executor): EventLoopGroup =
    new NioEventLoopGroup(numWorkers, executor)

}
