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
  def length = underlying.readableBytes

  override def toString = s"ChannelBufferBuf($underlying)"

  def write(bytes: Array[Byte], off: Int): Unit = {
    val dup = underlying.duplicate()
    dup.readBytes(bytes, off, dup.readableBytes)
  }

  def slice(i: Int, j: Int): Buf = {
    require(i >=0 && j >= 0, "Index out of bounds")

    if (j <= i || i >= length) Buf.Empty
    else if (i == 0 && j >= length) this
    else new ChannelBufferBuf(underlying.slice(i, (j-i) min (length-i)))
  }
  
  override def equals(other: Any): Boolean = other match {
    case ChannelBufferBuf(otherCB) => underlying.equals(otherCB)
    case other: Buf =>  Buf.equals(this, other)
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

  /**
   * Obtain a buffer using the provided ChannelBuffer unsafely.
   */
  @deprecated("Use ChannelBufferBuf.Shared, ChannelBufferBuf.Owned.", "6.23.0")
  def apply(cb: ChannelBuffer): Buf = Owned(cb.duplicate)

  /** Extract a read-only ChannelBuffer from a ChannelBufferBuf. */
  def unapply(cbb: ChannelBufferBuf): Option[ChannelBuffer] =
    Some(ChannelBuffers.unmodifiableBuffer(cbb.underlying))

  /**
   * Coerce a buf to a ChannelBufferBuf
   */
  def coerce(buf: Buf): ChannelBufferBuf = buf match {
    case buf: ChannelBufferBuf => buf
    case buf if buf.isEmpty => ChannelBufferBuf.Empty
    case buf =>
      val Buf.ByteArray.Owned(bytes, begin, end) = Buf.ByteArray.coerce(buf)
      val cb = ChannelBuffers.wrappedBuffer(bytes, begin, end - begin)
      new ChannelBufferBuf(cb)
  }

  object Owned {

    // N.B. We cannot use ChannelBuffers.unmodifiableBuffer to ensure
    // correctness because it prevents direct access to its underlying byte
    // array.

    /**
     * Obtain a buffer using the provided ChannelBuffer, which should not be
     * mutated after being passed to this function.
     */
    def apply(cb: ChannelBuffer): Buf = cb match {
      case cb if cb.readableBytes == 0 => Buf.Empty
      case BufChannelBuffer(buf) => buf
      case cb => new ChannelBufferBuf(cb)
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
