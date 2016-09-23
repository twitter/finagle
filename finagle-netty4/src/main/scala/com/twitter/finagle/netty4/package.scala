package com.twitter.finagle

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.util.ProxyThreadFactory
import com.twitter.util.Awaitable
import io.netty.buffer.{UnpooledByteBufAllocator, ByteBufAllocator}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.util.concurrent.Executors

/**
 * Package netty4 implements the bottom finagle primitives:
 * [[com.twitter.finagle.Server]] and a client transport in terms of
 * the netty4 event loop.
 */
package object netty4 {

  // this forces netty to use a "cleaner" for direct byte buffers
  // which we need as long as we don't release them.
  System.setProperty("io.netty.maxDirectMemory", "0")

  // nb: we can't use io.netty.buffer.UnpooledByteBufAllocator.DEFAULT
  //     because we need to disable the leak-detector and
  //     because we don't prefer direct byte buffers.
  //
  // See CSL-3027 for more details.
  private[netty4] val UnpooledAllocator = new UnpooledByteBufAllocator(
    /* preferDirect */ false,
    /* disableLeakDetector */ true
  )

  object param {

    private[netty4] case class Allocator(allocator: ByteBufAllocator)
    private[netty4] implicit object Allocator extends Stack.Param[Allocator] {
      // TODO investigate pooled allocator CSL-2089
      override val default: Allocator = Allocator(UnpooledAllocator)
    }

    /**
     * A class eligible for configuring the [[io.netty.channel.EventLoopGroup]] used
     * to execute I/O work for finagle clients and servers. The default is global and shared
     * among clients and servers such that we can inline work on the I/O threads. Modifying
     * the default has performance and instrumentation implications and should only be
     * done so with care. If there is particular work you would like to schedule off
     * the I/O threads, consider scheduling that work on a separate thread pool
     * more granularly (e.g. [[com.twitter.util.FuturePool]] is a good tool for this).
     */
    case class WorkerPool(eventLoopGroup: EventLoopGroup)
    implicit object WorkerPool extends Stack.Param[WorkerPool] {
      override val default: WorkerPool = {
        val threadFactory = new ProxyThreadFactory(
          new NamedPoolThreadFactory("finagle/netty4", makeDaemons = true),
          ProxyThreadFactory.newProxiedRunnable(
            () => Awaitable.enableBlockingTimeTracking(),
            () => Awaitable.disableBlockingTimeTracking()
          )
        )
        // Netty will create `numWorkers` children in the `NioEventLoopGroup` (which
        // in this case are of type `NioEventLoop`). Each `NioEventLoop` will pin itself
        // to a thread acquired from the `executor` and will multiplex over channels.
        // Thus, with this configuration, we should not acquire more than `numWorkers`
        // threads from the `executor`.
        val executor = Executors.newCachedThreadPool(threadFactory)
        val eventLoopGroup = new NioEventLoopGroup(numWorkers(), executor)
        WorkerPool(eventLoopGroup)
      }
    }
  }
}
