package com.twitter.finagle.netty4

import com.twitter.io.Buf
import io.netty.buffer._

private[finagle] object BufAsByteBuf {
  object Owned {
    /**
     * A read-only and potentially non-copying `ByteBuf` wrapper for [[Buf]].
     */
    def apply(buf: Buf): ByteBuf = {
      val bb = buf match {
        case _ if buf.isEmpty =>
          Unpooled.EMPTY_BUFFER
        case ByteBufAsBuf.Owned(underlying) =>
          underlying
        case _: Buf.ByteArray =>
          Unpooled.wrappedBuffer(Buf.ByteArray.Owned.extract(buf))
        case _ =>
          Unpooled.wrappedBuffer(Buf.ByteBuffer.Owned.extract(buf))
      }

      Unpooled.unmodifiableBuffer(bb)
    }

  }

  object Shared {
    /**
     * A read-only copying `ByteBuf` wrapper for [[Buf]]
     */
    def apply(buf: Buf): ByteBuf = {
      val bb = buf match {
        case _ if buf.isEmpty =>
          Unpooled.EMPTY_BUFFER
        case ByteBufAsBuf.Shared(underlying) =>
          Unpooled.copiedBuffer(underlying)
        case _: Buf.ByteArray =>
          Unpooled.wrappedBuffer(Buf.ByteArray.Shared.extract(buf))
        case _ =>
          Unpooled.wrappedBuffer(Buf.ByteBuffer.Shared.extract(buf))
      }

      Unpooled.unmodifiableBuffer(bb)
    }
  }
}
