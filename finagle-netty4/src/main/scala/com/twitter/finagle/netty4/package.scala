package com.twitter.finagle

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}
import io.netty.buffer.UnpooledByteBufAllocator

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

  // We set a sane default and reject client initiated TLS/SSL session
  // renegotiations (for security reasons).
  //
  // NOTE: This property affects both JDK SSL (Java 8+) and Netty 4 OpenSSL
  // implementations.
  if (System.getProperty("jdk.tls.rejectClientInitiatedRenegotiation") == null) {
    System.setProperty("jdk.tls.rejectClientInitiatedRenegotiation", "true")
  }

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
  //     because we don't prefer direct byte buffers.
  //
  // See CSL-3027 for more details.
  private[netty4] val UnpooledAllocator = new UnpooledByteBufAllocator(
    /* preferDirect */ false,
    /* disableLeakDetector */ !trackReferenceLeaks.enabled
  )

  private[finagle] val DirectToHeapInboundHandlerName = "directToHeap"
}
