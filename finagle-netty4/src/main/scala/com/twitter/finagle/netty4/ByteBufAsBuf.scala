package com.twitter.finagle.netty4

import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import io.netty.buffer._

private[finagle] object ByteBufAsBuf {

  object Owned {
    /**
     * Construct a [[Buf]] wrapper for ``ByteBuf``.
     *
     * @note this wrapper does not support ref-counting and therefore should either
     *       be used with unpooled and non-leak detecting allocators or managed
     *       via the ref-counting methods of the wrapped `buf`. Non-empty buffers
     *       are `retain`ed.
     */
    def apply(buf: ByteBuf): Buf =
      if (buf.readableBytes == 0)
        Buf.Empty
      else
        new ByteBufAsBuf(buf)

    /**
     * Extract a [[ByteBufAsBuf]]'s underlying ByteBuf without copying.
     */
    def unapply(wrapped: ByteBufAsBuf): Option[ByteBuf] = Some(wrapped.underlying)

    /**
     * Extract a read-only `ByteBuf` from [[Buf]] potentially without copying.
     */
    def extract(buf: Buf): ByteBuf = BufAsByteBuf.Owned(buf)
  }

  object Shared {
    /**
     * Construct a [[Buf]] by copying `ByteBuf`.
     */
    def apply(buf: ByteBuf): Buf =
      if (buf.readableBytes == 0)
        Buf.Empty
      else
        new ByteBufAsBuf(buf.copy())

    /**
     * Extract a copy of the [[ByteBufAsBuf]]'s underlying ByteBuf.
     */
    def unapply(wrapped: ByteBufAsBuf): Option[ByteBuf] = Some(wrapped.underlying.copy())

    /**
     * Extract a read-only `ByteBuf` copy from [[Buf]].
     */
    def extract(buf: Buf): ByteBuf = BufAsByteBuf.Shared(buf)
  }
}

/**
 * a [[Buf]] wrapper for Netty `ByteBuf`s.
 */
private[finagle] class ByteBufAsBuf(
    private[finagle] val underlying: ByteBuf)
  extends Buf {
  // nb: `underlying` is exposed for testing

  def write(bytes: Array[Byte], off: Int): Unit = {
    checkWriteArgs(bytes.length, off)
    val dup = underlying.duplicate()
    dup.readBytes(bytes, off, dup.readableBytes)
  }

  protected def unsafeByteArrayBuf: Option[ByteArray] =
    if (underlying.hasArray) {
      val bytes = underlying.array
      val begin = underlying.arrayOffset + underlying.readerIndex
      val end = begin + underlying.readableBytes
      Some(new Buf.ByteArray(bytes, begin, end))
    } else None

  def length: Int = underlying.readableBytes

  def slice(from: Int, until: Int): Buf = {
    checkSliceArgs(from, until)
    if (isSliceEmpty(from, until)) Buf.Empty
    else if (isSliceIdentity(from, until)) this
    else new ByteBufAsBuf(underlying.slice(from, Math.min(until - from, length - from)))
  }

  override def equals(other: Any): Boolean = other match {
    case ByteBufAsBuf.Owned(otherBB) => underlying.equals(otherBB)
    case other: Buf => Buf.equals(this, other)
    case _ => false
  }
}
