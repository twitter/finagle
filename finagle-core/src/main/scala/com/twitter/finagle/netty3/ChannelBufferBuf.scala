package com.twitter.finagle.netty3

import com.twitter.io.Buf
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

/**
 * A [[com.twitter.io.Buf]] wrapper for
 * Netty [[org.jboss.netty.buffer.ChannelBuffer ChannelBuffers]].
 *
 * @note Since `ChannelBuffer`s are mutable, modifying the wrapped buffer
 * within `slice`s of a `ChannelBufferBuf` will modify the original wrapped
 * `ChannelBuffer`. Similarly, modifications to the original buffer will be
 * reflected in slices.
 *
 * @param underlying The [[org.jboss.netty.buffer.ChannelBuffer]] to be wrapped in a
 * [[com.twitter.io.Buf]] interface.
 */
class ChannelBufferBuf(protected val underlying: ChannelBuffer) extends Buf {
  def length: Int = underlying.readableBytes

  override def toString: String = s"ChannelBufferBuf($underlying)"

  def get(index: Int): Byte = {
    val pos = underlying.readerIndex + index
    underlying.getByte(pos)
  }

  def process(from: Int, until: Int, processor: Buf.Processor): Int = {
    checkSliceArgs(from, until)
    if (isSliceEmpty(from, until)) return -1
    val off = underlying.readerIndex + from
    val endAt = math.min(length, underlying.readerIndex + until)
    var i = 0
    var continue = true
    while (continue && i < endAt) {
      val byte = underlying.getByte(off + i)
      if (processor(byte))
        i += 1
      else
        continue = false
    }
    if (continue) -1
    else from + i
  }

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

  def slice(i: Int, j: Int): Buf = {
    checkSliceArgs(i, j)
    if (isSliceEmpty(i, j)) Buf.Empty
    else if (isSliceIdentity(i, j)) this
    else {
      val from = i + underlying.readerIndex
      val until = math.min(j-i, length-i)
      new ChannelBufferBuf(underlying.slice(from, until))
    }
  }

  override def equals(other: Any): Boolean = other match {
    case ChannelBufferBuf(otherCB) => underlying.equals(otherCB)
    case other: Buf => Buf.equals(this, other)
    case _ => false
  }

  protected def unsafeByteArrayBuf: Option[Buf.ByteArray] =
    if (underlying.hasArray) {
      val bytes = underlying.array
      val begin = underlying.arrayOffset + underlying.readerIndex
      val end = begin + underlying.readableBytes
      Some(new Buf.ByteArray(bytes, begin, end))
    } else None
}

object ChannelBufferBuf {

  private val Empty = new ChannelBufferBuf(ChannelBuffers.EMPTY_BUFFER)

  /** Extract a read-only ChannelBuffer from a ChannelBufferBuf. */
  def unapply(cbb: ChannelBufferBuf): Option[ChannelBuffer] =
    Some(ChannelBuffers.unmodifiableBuffer(cbb.underlying))

  /**
   * Coerce a buf to a ChannelBufferBuf
   */
  def coerce(buf: Buf): ChannelBufferBuf = buf match {
    case buf: ChannelBufferBuf => buf
    case _ if buf.isEmpty => ChannelBufferBuf.Empty
    case _ =>
      val Buf.ByteArray.Owned(bytes, begin, end) = Buf.ByteArray.coerce(buf)
      val cb = ChannelBuffers.wrappedBuffer(bytes, begin, end - begin)
      new ChannelBufferBuf(cb)
  }

  /**
   * Java API for [[ChannelBufferBuf.Owned.apply]].
   */
  def newOwned(cb: ChannelBuffer): Buf =
    Owned(cb)

  object Owned {

    // N.B. We cannot use ChannelBuffers.unmodifiableBuffer to ensure
    // correctness because it prevents direct access to its underlying byte
    // array.

    /**
     * Obtain a buffer using the provided ChannelBuffer, which should not be
     * mutated after being passed to this function.
     *
     * @see [[newOwned]] for a Java friendly API.
     */
    def apply(cb: ChannelBuffer): Buf = cb match {
      case _ if cb.readableBytes == 0 => Buf.Empty
      case BufChannelBuffer(buf) => buf
      case _ => new ChannelBufferBuf(cb)
    }

    /** Extract the buffer's underlying ChannelBuffer. It should not be mutated. */
    def unapply(cbb: ChannelBufferBuf): Option[ChannelBuffer] = Some(cbb.underlying)

    def extract(buf: Buf): ChannelBuffer = ChannelBufferBuf.coerce(buf).underlying
  }

  object Shared {
    def apply(cb: ChannelBuffer): Buf = Owned(cb.copy)
    def unapply(cbb: ChannelBufferBuf): Option[ChannelBuffer] = Owned.unapply(cbb).map(_.copy)
    def extract(buf: Buf): ChannelBuffer =  Owned.extract(buf).copy
  }
}
