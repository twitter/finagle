package com.twitter.finagle.netty4

import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import io.netty.buffer._
import io.netty.util.ByteProcessor

private[finagle] object ByteBufAsBuf {

  // Assuming that bb.hasArray.
  private final def heapToBuf(bb: ByteBuf): Buf.ByteArray = {
    val begin = bb.arrayOffset + bb.readerIndex
    val end = begin + bb.readableBytes
    new Buf.ByteArray(bb.array, begin, end)
  }

  /**
   * Process `bb` 1-byte at a time using the given
   * [[Buf.Processor]], starting at index `from` of `bb` until
   * index `until`. Processing will halt if the processor
   * returns `false` or after processing the final byte.
   *
   * @return -1 if the processor processed all bytes or
   *         the last processed index if the processor returns
   *         `false`.
   *         Will return -1 if `from` is greater than or equal to
   *         `until` or `length` of the underlying buffer.
   *         Will return -1 if `until` is greater than or equal to
   *         `length` of the underlying buffer.
   *
   * @param from the starting index, inclusive. Must be non-negative.
   *
   * @param until the ending index, exclusive. Must be non-negative.
   */
  def process(from: Int, until: Int, processor: Buf.Processor, bb: ByteBuf): Int = {
    Buf.checkSliceArgs(from, until)

    val length = bb.readableBytes()

    // check if chunk to process is empty
    if (until <= from || from >= length) -1
    else {
      val byteProcessor = new ByteProcessor {
        def process(value: Byte): Boolean = processor(value)
      }
      val readerIndex = bb.readerIndex()
      val off = readerIndex + from
      val len = math.min(length - from, until - from)
      val index = bb.forEachByte(off, len, byteProcessor)
      if (index == -1) -1
      else index - readerIndex
    }
  }

  /**
   * Construct a [[Buf]] wrapper for `ByteBuf`.
   *
   * @note this wrapper does not support ref-counting and therefore should either
   *       be used with unpooled and non-leak detecting allocators or managed
   *       via the ref-counting methods of the wrapped `buf`. Non-empty buffers
   *       are `retain`ed.
   *
   * @note if the given is backed by a heap array, it will be coerced into `Buf.ByteArray`
   *       and then released. This basically means it's only safe to use this smart constructor
   *       with unpooled heap buffers.
   */
  def apply(buf: ByteBuf): Buf =
    if (buf.readableBytes == 0) Buf.Empty
    else if (buf.hasArray)
      try heapToBuf(buf)
      finally buf.release()
    else new ByteBufAsBuf(buf)

  /**
   * Extract a [[ByteBufAsBuf]]'s underlying ByteBuf without copying.
   */
  def unapply(wrapped: ByteBufAsBuf): Option[ByteBuf] = Some(wrapped.underlying)

  /**
   * Extract a read-only `ByteBuf` from [[Buf]] potentially without copying.
   */
  def extract(buf: Buf): ByteBuf = BufAsByteBuf(buf)
}

/**
 * a [[Buf]] wrapper for Netty `ByteBuf`s.
 */
private[finagle] class ByteBufAsBuf(private[finagle] val underlying: ByteBuf) extends Buf {
  // nb: `underlying` is exposed for testing

  def get(index: Int): Byte =
    underlying.getByte(underlying.readerIndex() + index)

  def process(from: Int, until: Int, processor: Buf.Processor): Int =
    ByteBufAsBuf.process(from, until, processor, underlying)

  def write(bytes: Array[Byte], off: Int): Unit = {
    checkWriteArgs(bytes.length, off)
    val dup = underlying.duplicate()
    dup.readBytes(bytes, off, dup.readableBytes)
  }

  def write(buffer: java.nio.ByteBuffer): Unit = {
    checkWriteArgs(buffer.remaining, 0)
    val dup = underlying.duplicate()
    val currentLimit = buffer.limit
    buffer.limit(buffer.position + length)
    dup.readBytes(buffer)
    buffer.limit(currentLimit)
  }

  protected def unsafeByteArrayBuf: Option[ByteArray] =
    if (underlying.hasArray) Some(ByteBufAsBuf.heapToBuf(underlying))
    else None

  def length: Int = underlying.readableBytes

  def slice(from: Int, until: Int): Buf = {
    Buf.checkSliceArgs(from, until)
    if (isSliceEmpty(from, until)) Buf.Empty
    else if (isSliceIdentity(from, until)) this
    else {
      val off = underlying.readerIndex() + from
      val len = Math.min(length, until) - from
      new ByteBufAsBuf(underlying.slice(off, len))
    }
  }

  override def equals(other: Any): Boolean = other match {
    case ByteBufAsBuf(otherBB) => underlying.equals(otherBB)
    case composite: Buf.Composite =>
      // Composite.apply has a relatively high overhead, so let it probe
      // back into this Buf.
      composite == this
    case other: Buf if other.length == length =>
      val proc = new ByteProcessor {
        private[this] var pos = 0
        def process(value: Byte): Boolean = {
          if (other.get(pos) == value) {
            pos += 1
            true
          } else {
            false
          }
        }
      }
      underlying.forEachByte(underlying.readerIndex(), length, proc) == -1

    case _ => false
  }
}
