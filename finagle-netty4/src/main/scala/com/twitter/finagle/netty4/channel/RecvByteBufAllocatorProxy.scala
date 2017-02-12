package com.twitter.finagle.netty4.channel

import io.netty.buffer.{ByteBuf, ByteBufAllocator}
import io.netty.channel.AdaptiveRecvByteBufAllocator
import io.netty.channel.RecvByteBufAllocator.{DelegatingHandle, Handle}

/**
 * A version of [[AdaptiveRecvByteBufAllocator]] that proxies allocation
 * requests to the given `allocator`.
 */
private[netty4] class RecvByteBufAllocatorProxy(
    allocator: ByteBufAllocator)
  extends AdaptiveRecvByteBufAllocator {

  override def newHandle(): Handle = new DelegatingHandle(super.newHandle()) {
    override def allocate(alloc: ByteBufAllocator): ByteBuf =
      delegate().allocate(allocator)
  }
}
