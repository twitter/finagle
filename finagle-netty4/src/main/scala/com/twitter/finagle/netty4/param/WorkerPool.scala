package com.twitter.finagle.netty4.param

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.WorkerEventLoop
import io.netty.channel.EventLoopGroup
import java.util.concurrent.Executor

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
    this(WorkerEventLoop.make(executor, numWorkers))

  def mk(): (WorkerPool, Stack.Param[WorkerPool]) =
    (this, WorkerPool.workerPoolParam)
}

object WorkerPool {
  implicit val workerPoolParam: Stack.Param[WorkerPool] =
    Stack.Param(WorkerPool(WorkerEventLoop.Global))
}
