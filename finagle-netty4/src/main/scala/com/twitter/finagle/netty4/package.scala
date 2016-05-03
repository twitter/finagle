package com.twitter.finagle

import com.twitter.app.GlobalFlag
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.util.ProxyThreadFactory
import com.twitter.jvm.numProcs
import com.twitter.util.Awaitable
import io.netty.buffer.{UnpooledByteBufAllocator, ByteBufAllocator}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.internal.PlatformDependent
import java.util.concurrent.{ExecutorService, Executors}

/**
 * Package netty4 implements the bottom finagle primitives:
 * [[com.twitter.finagle.Server]] and a client transport in terms of
 * the netty4 event loop.
 */
package object netty4 {

  object numWorkers extends GlobalFlag((numProcs() * 2).ceil.toInt, "number of netty4 worker threads")

  // global worker thread pool for finagle clients and servers.
  private[netty4] val Executor: ExecutorService = {
    val threadFactory = new ProxyThreadFactory(
      new NamedPoolThreadFactory("finagle/netty4", makeDaemons = true),
      ProxyThreadFactory.newProxiedRunnable(
        () => Awaitable.enableBlockingTimeTracking(),
        () => Awaitable.disableBlockingTimeTracking()
      )
    )
    Executors.newCachedThreadPool(threadFactory)
  }

  private[netty4] object WorkerPool extends NioEventLoopGroup(numWorkers(), Executor)

  // nb: we can't use io.netty.buffer.UnpooledByteBufAllocator.DEFAULT
  //     because we need to disable the leak-detector.
  private[netty4] val UnpooledAllocator = new UnpooledByteBufAllocator(
    /* preferDirect */ PlatformDependent.directBufferPreferred(),
    /* disableLeakDetector */ true
  )

  object param {

    private[netty4] case class Allocator(allocator: ByteBufAllocator)
    private[netty4] implicit object Allocator extends Stack.Param[Allocator] {
      // TODO investigate pooled allocator CSL-2089
      override val default: Allocator = Allocator(UnpooledAllocator)
    }
  }
}
