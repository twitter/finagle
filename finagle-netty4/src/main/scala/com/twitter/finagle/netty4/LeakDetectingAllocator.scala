package com.twitter.finagle.netty4

import io.netty.buffer.{AbstractByteBufAllocator, ByteBuf, CompositeByteBuf, UnpooledByteBufAllocator}

/**
 * Unpooled allocator which produces composite byte bufs which permit only onheap/unpooled components.
 *
 * Used to work-around netty leak detection nonsense.
 */
private[netty4] object LeakDetectingAllocator extends AbstractByteBufAllocator(false /*preferDirect*/) {

  private[this] val DefaultMaxComponents = 16 // io.netty.buffer.AbstractByteBufAllocator.DEFAULT_MAX_COMPONENTS

  private[this] val underlying = new UnpooledByteBufAllocator(/* preferDirect */ false)

  def newDirectBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
    underlying.directBuffer(initialCapacity, maxCapacity)

  def newHeapBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
    underlying.heapBuffer(initialCapacity, maxCapacity)

  def isDirectBufferPooled: Boolean = underlying.isDirectBufferPooled


  override def compositeHeapBuffer(): CompositeByteBuf =
    new LeakSafeCompositeByteBuf(this, false, DefaultMaxComponents)

  override def compositeHeapBuffer(maxNumComponents: Int): CompositeByteBuf =
    new LeakSafeCompositeByteBuf(this, false, maxNumComponents)

  override def compositeDirectBuffer(): CompositeByteBuf =
    new LeakSafeCompositeByteBuf(this, true, DefaultMaxComponents)

  override def compositeDirectBuffer(maxNumComponents: Int): CompositeByteBuf =
    new LeakSafeCompositeByteBuf(this, true, maxNumComponents)

}
