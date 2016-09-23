package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag

/**
 * An experimental option that enables pooling for receive buffers.
 *
 * Since we always copy onto the heap (see `DirectToHeapInboundHandler`), the receive
 * buffers never leave the pipeline hence can safely be pooled.
 * In its current form, this will preallocate at least N * 2 mb (chunk size) of
 * direct memory at the application startup, where N is the number of worker threads
 * Finagle uses.
 *
 * Example:
 *
 * On a 16 core machine, the lower bound for the pool size will be 16 * 2 * 2mb = 64mb.
 *
 * @note This will likely be a default for finagle-netty4.
 */
object poolReceiveBuffers extends GlobalFlag(false, "enables/disables pooling of receive buffers")