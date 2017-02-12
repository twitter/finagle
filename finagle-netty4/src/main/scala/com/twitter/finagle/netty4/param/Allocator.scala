package com.twitter.finagle.netty4.param

import com.twitter.finagle.netty4.UnpooledAllocator
import com.twitter.finagle.Stack
import io.netty.buffer.ByteBufAllocator

private[netty4] case class Allocator(allocator: ByteBufAllocator)
private[netty4] object Allocator {
  // TODO investigate pooled allocator CSL-2089
  // While we already pool receive buffers, this ticket is about end-to-end pooling
  // (everything in the pipeline should be pooled).
  implicit val allocatorParam: Stack.Param[Allocator] =
    Stack.Param(Allocator(UnpooledAllocator))
}

