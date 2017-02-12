package com.twitter.finagle.netty4

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle

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
