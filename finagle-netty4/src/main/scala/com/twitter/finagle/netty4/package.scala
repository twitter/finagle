package com.twitter.finagle

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.util.ProxyThreadFactory
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap, Toggle}
import com.twitter.util.Awaitable
import io.netty.buffer.{ByteBufAllocator, UnpooledByteBufAllocator}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.util.concurrent.Executors

/**
 * Package netty4 implements the bottom finagle primitives:
 * [[com.twitter.finagle.Server]] and a client transport in terms of
 * the netty4 event loop.
 */
package object netty4 {

  /**
   * The [[ToggleMap]] used for finagle-netty4.
   */
  private[finagle] val Toggles: ToggleMap =
    StandardToggleMap("com.twitter.finagle.netty4", DefaultStatsReceiver)

  /**
   * An experimental option that enables pooling for receive buffers.
   *
   * Since we always copy onto the heap (see `DirectToHeapInboundHandler`), the receive
   * buffers never leave the pipeline hence can safely be pooled.
   * In its current form, this will preallocate at least N * 128kb (chunk size) of
   * direct memory at the application startup, where N is the number of worker threads
   * Finagle uses.
   *
   * Example:
   *
   * On a 16 core machine, the lower bound for the pool size will be 16 * 2 * 128kb = 4mb.
   */
  private[netty4] object poolReceiveBuffers {
    private[this] val underlying: Toggle[Int] =
      Toggles("com.twitter.finagle.netty4.poolReceiveBuffers")

    /**
     * Checks (via a toggle) if pooling of receive buffers is enabled on this instanace.
     */
    def apply(): Boolean = underlying(ServerInfo().id.hashCode)
  }

  // this forces netty to use a "cleaner" for direct byte buffers
  // which we need as long as we don't release them.
  System.setProperty("io.netty.maxDirectMemory", "0")

  // We allocate one arena per a worker thread to reduce contention. By default
  // this will be equal to the number of logical cores * 2.
  //
  // NOTE: Before overriding it, we check whether or not it was set before. This way users
  // will have a chance to tune it.
  //
  // NOTE: Only applicable when pooling is enabled (see `poolReceiveBuffers`).
  if (System.getProperty("io.netty.allocator.numDirectArenas") == null) {
    System.setProperty("io.netty.allocator.numDirectArenas", numWorkers().toString)
  }

  // This determines the size of the memory chunks we allocate in arenas. Netty's default
  // is 16mb, we shrink it to 128kb.
  //
  // We make the trade-off between an initial memory footprint and the max buffer size
  // that can still be pooled (assuming that 128kb is big enough to cover nearly all
  // inbound messages sent over TCP). Every allocation that exceeds 128kb will fall back
  // to an unpooled allocator.
  //
  // The `io.netty.allocator.maxOrder` (default: 4) determines the number of left binary
  // shifts we need to apply to the `io.netty.allocator.pageSize` (default: 8192):
  // 8192 << 4 = 128kb.
  //
  // NOTE: Before overriding it, we check whether or not it was set before. This way users
  // will have a chance to tune it.
  //
  // NOTE: Only applicable when pooling is enabled (see `poolReceiveBuffers`).
  if (System.getProperty("io.netty.allocator.maxOrder") == null) {
    System.setProperty("io.netty.allocator.maxOrder", "4")
  }

  // nb: we can't use io.netty.buffer.UnpooledByteBufAllocator.DEFAULT
  //     because we need to disable the leak-detector and
  //     because we don't prefer direct byte buffers.
  //
  // See CSL-3027 for more details.
  private[netty4] val UnpooledAllocator = new UnpooledByteBufAllocator(
    /* preferDirect */ false,
    /* disableLeakDetector */ true
  )

  private[finagle] val DirectToHeapInboundHandlerName = "direct to heap"

  object param {

    private[netty4] case class Allocator(allocator: ByteBufAllocator)
    private[netty4] implicit object Allocator extends Stack.Param[Allocator] {
      // TODO investigate pooled allocator CSL-2089
      // While we already pool receive buffers, this ticket is about end-to-end pooling
      // (everything in the pipeline should be pooled).
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
