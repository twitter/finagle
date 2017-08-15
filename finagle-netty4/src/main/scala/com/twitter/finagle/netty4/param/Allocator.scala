package com.twitter.finagle.netty4.param

import com.twitter.finagle.netty4.{LeakDetectingAllocator, trackReferenceLeaks}
import com.twitter.finagle.Stack
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator}

private[netty4] case class Allocator(allocator: ByteBufAllocator)
private[netty4] object Allocator {
  implicit val allocatorParam: Stack.Param[Allocator] = Stack.Param(
    Allocator(
      if (trackReferenceLeaks()) LeakDetectingAllocator
      else PooledByteBufAllocator.DEFAULT
    )
  )
}
