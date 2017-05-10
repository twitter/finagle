package com.twitter.finagle.netty4

import io.netty.buffer.{PooledByteBufAllocator, Unpooled, UnpooledByteBufAllocator}
import org.scalatest.FunSuite

class LeakSafeCompositeByteBufTest extends FunSuite {

  val IllegalInstances = Seq(
    "pooled-heap" -> PooledByteBufAllocator.DEFAULT.heapBuffer(),
    "pooled-direct" -> PooledByteBufAllocator.DEFAULT.directBuffer(),
    "unpooled-direct" -> Unpooled.directBuffer()
  )

  IllegalInstances.foreach { case (k, v) =>
    test(s"$k: throws exceptions on direct buffers") {
      val comp = new LeakSafeCompositeByteBuf (UnpooledByteBufAllocator.DEFAULT, true, 10)
      intercept[IllegalArgumentException] { comp.addComponents(v) }
    }
  }
}
