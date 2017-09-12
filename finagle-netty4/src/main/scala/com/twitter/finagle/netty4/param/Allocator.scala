package com.twitter.finagle.netty4.param

import com.twitter.finagle.netty4.{LeakDetectingAllocator, trackReferenceLeaks}
import com.twitter.finagle.Stack
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator}

private[finagle] case class Allocator(allocator: ByteBufAllocator)
private[finagle] object Allocator {
  implicit val allocatorParam: Stack.Param[Allocator] = Stack.Param(
    Allocator(
      if (trackReferenceLeaks()) LeakDetectingAllocator
      else PooledByteBufAllocator.DEFAULT
    )
  )
}
