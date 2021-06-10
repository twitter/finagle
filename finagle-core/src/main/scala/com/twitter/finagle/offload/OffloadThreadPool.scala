package com.twitter.finagle.offload

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import java.util.concurrent.{
  ExecutorService,
  LinkedBlockingQueue,
  RejectedExecutionHandler,
  ThreadPoolExecutor,
  TimeUnit
}

private object OffloadThreadPool {

  /** Construct an `ExecutorService` with the proper thread names and metrics */
  def apply(poolSize: Int, queueSize: Int, stats: StatsReceiver): ExecutorService =
    new ThreadPoolExecutor(
      poolSize /*corePoolSize*/,
      poolSize /*maximumPoolSize*/,
      0L /*keepAliveTime*/,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](queueSize) /*workQueue*/,
      new NamedPoolThreadFactory("finagle/offload", makeDaemons = true) /*threadFactory*/,
      new RunsOnNettyThread(stats.counter("not_offloaded_tasks")))

  // This handler is run when the submitted work is rejected from the ThreadPool, usually because
  // its work queue has reached the proposed limit. When that happens, we simply run the work on
  // the current thread (a thread that was trying to offload), which is most commonly a Netty IO
  // worker.
  private[this] final class RunsOnNettyThread(rejections: Counter)
      extends RejectedExecutionHandler {
    def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = {
      if (!e.isShutdown) {
        rejections.incr()
        r.run()
      }
    }
  }
}
