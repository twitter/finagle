package com.twitter.finagle.netty4

import io.netty.buffer.{ByteBuf, ByteBufAllocator, CompositeByteBuf}
import java.lang.Iterable
import scala.collection.JavaConverters._

private[netty4] object LeakSafeCompositeByteBuf {
  // PooledByteBuf is protected so we have to reflect
  val pooledClass = Class.forName("io.netty.buffer.PooledByteBuf")
}
/**
 * A `CompositeByteBuf` instance which only accepts on-heap and unpooled ByteBuf components. For
 * use in conjunction with [[LeakDetectingAllocator]] strictly for correctness testing. No attempt
 * for performance optimization has been made so use in production at your own risk.
 */
private[netty4] class LeakSafeCompositeByteBuf(alloc: ByteBufAllocator, direct: Boolean, maxNumComponents: Int)
  extends CompositeByteBuf(alloc, direct, maxNumComponents) {
  import LeakSafeCompositeByteBuf.pooledClass

  val isDirectOrPooled: ByteBuf => Boolean = { buf =>
    buf.isDirect || pooledClass.isAssignableFrom(buf.getClass)
  }

  override def addComponents(buffers: ByteBuf*): CompositeByteBuf = {
    if (buffers.exists(isDirectOrPooled)) throw new IllegalArgumentException("no direct or pooled byte buffers permitted")
    super.addComponents(buffers: _*)
  }

  override def addComponents(buffers: Iterable[ByteBuf]): CompositeByteBuf = {
    if (buffers.asScala.exists(isDirectOrPooled)) throw new IllegalArgumentException("no direct or pooled byte buffers permitted")
    super.addComponents(buffers)
  }

  override def addComponents(
    increaseWriterIndex: Boolean,
    buffers: ByteBuf*
  ): CompositeByteBuf = {
    if (buffers.exists(isDirectOrPooled)) throw new IllegalArgumentException("no direct or pooled byte buffers permitted")
    super.addComponents(increaseWriterIndex, buffers: _*)
  }

  override def addComponents(
    increaseWriterIndex: Boolean,
    buffers: Iterable[ByteBuf]
  ): CompositeByteBuf = {
    if (buffers.asScala.exists(isDirectOrPooled)) throw new IllegalArgumentException("no direct or pooled byte buffers permitted")
    super.addComponents(increaseWriterIndex, buffers)
  }

  override def addComponents(
    cIndex: Int,
    buffers: ByteBuf*
  ): CompositeByteBuf = {
    if (buffers.exists(isDirectOrPooled)) throw new IllegalArgumentException("no direct or pooled byte buffers permitted")
    super.addComponents(cIndex, buffers: _*)
  }

  override def addComponents(
    cIndex: Int,
    buffers: Iterable[ByteBuf]
  ): CompositeByteBuf = {
    if (buffers.asScala.exists(isDirectOrPooled)) throw new IllegalArgumentException("no direct or pooled byte buffers permitted")
    super.addComponents(cIndex, buffers)
  }
}
