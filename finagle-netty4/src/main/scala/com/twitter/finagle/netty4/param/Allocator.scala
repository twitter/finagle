package com.twitter.finagle.netty4.param

import com.twitter.app.GlobalFlag
import com.twitter.finagle.Stack
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator, UnpooledByteBufAllocator}

/**
 * Flag for configuring the default Netty4 `ByteBufAllocator`
 *
 * @note This flag is intended only for testing and emergency purposes. Outside of those
 *       two use cases it will likely only serve to degrade performance and therefore is
 *       not a recommended tuning parameter for normal operation.
 */
private object useUnpooledByteBufAllocator extends GlobalFlag[Boolean](
  default = false,
  help = "Use an unpooled Netty4 ByteBuf allocator as the default allocator " +
    "instead of the pooled ByteBuf allocator.")

private[finagle] case class Allocator(allocator: ByteBufAllocator)
private[finagle] object Allocator {

  private def defaultAllocator: ByteBufAllocator = {
    if (useUnpooledByteBufAllocator()) new UnpooledByteBufAllocator( /* preferDirect */ false)
    else PooledByteBufAllocator.DEFAULT
  }

  implicit val allocatorParam: Stack.Param[Allocator] = Stack.Param(Allocator(defaultAllocator))
}
