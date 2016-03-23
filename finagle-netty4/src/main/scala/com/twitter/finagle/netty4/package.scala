package com.twitter.finagle

import com.twitter.app.GlobalFlag
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.jvm.numProcs
import io.netty.buffer.{UnpooledByteBufAllocator, ByteBufAllocator}
import io.netty.channel.nio.NioEventLoopGroup
import java.util.concurrent.{ExecutorService, Executors}

/**
 * Package netty4 implements the bottom finagle primitives:
 * [[com.twitter.finagle.Server]] and a client transport in terms of
 * the netty4 event loop.
 */
package object netty4 {

  object numWorkers extends GlobalFlag((numProcs() * 2).ceil.toInt, "number of netty4 worker threads")

  // global worker thread pool for finagle clients and servers.
  private[netty4] val Executor: ExecutorService = Executors.newCachedThreadPool(
    new NamedPoolThreadFactory("finagle/netty4", makeDaemons = true)
  )

  private[netty4] object WorkerPool extends NioEventLoopGroup(numWorkers(), Executor)

  object param {

    private[netty4] case class Allocator(allocator: ByteBufAllocator)
    private[netty4] implicit object Allocator extends Stack.Param[Allocator] {
      // TODO investigate pooled allocator CSL-2089
      override val default: Allocator = Allocator(UnpooledByteBufAllocator.DEFAULT)
    }
  }
}
