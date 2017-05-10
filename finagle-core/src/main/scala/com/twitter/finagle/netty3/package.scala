package com.twitter.finagle

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.util.BlockingTimeTrackingThreadFactory
import java.util.concurrent.{ExecutorService, Executors}
import org.jboss.netty.channel.socket.nio.NioWorkerPool
import org.jboss.netty.util.{ThreadNameDeterminer, ThreadRenamingRunnable}

/**
 * Package netty3 implements the bottom finagle primitives:
 * {{com.twitter.finagle.Server}} and a client transport in terms of
 * the netty3 event loop.
 *
 * Note: when {{com.twitter.finagle.builder.ClientBuilder}} and
 * {{com.twitter.finagle.builder.ServerBuilder}} are deprecated,
 * package netty3 can move into its own package, so that only the
 * (new-style) clients and servers that depend on netty3 bring it in.
 */
package object netty3 {
  // Disable Netty's thread name rewriting, to preserve the "finagle/netty3"
  // suffix specified below.
  ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT)

  val Executor: ExecutorService = {
    val threadFactory = new BlockingTimeTrackingThreadFactory(
      new NamedPoolThreadFactory("finagle/netty3", makeDaemons = true)
    )

    Executors.newCachedThreadPool(threadFactory)
  }

  val WorkerPool: NioWorkerPool = new NioWorkerPool(Executor, numWorkers())
}
