package com.twitter.finagle.util

import com.twitter.io.Buf
import java.lang.{Double => JDouble, Float => JFloat}


/**
 * [[BufWriter]]s allow efficient construction of a [[Buf]]. Concatenating
 * [[Buf]]s together is a common pattern for codecs (especially on the encode path),
 * but using the `concat` method on [[Buf]] results in a suboptimal
 * representation. In many cases, a [[BufWriter]] not only allows for a more
 * optimal representation (i.e., sequential accessible out regions), but also
 * allows  for writes to avoid allocations. What this means in practice is that
 * builders are stateful and keep track of a writer index. Assume that the
 * builder implementations are *not* threadsafe unless otherwise noted.
 */
private[finagle] trait BufWriter {
  /**
   * Write 8 bits of `b`. The remaining 24 bits are ignored.
   */
  def writeByte(b: Int): BufWriter

  /**
   * Write 16 bits from `s` in big-endian order. The remaining
   * 16 bits are ignored.
   */
  def writeShortBE(s: Int): BufWriter

  /**
   * Write 16 bits from `s` in little-endian order. The remaining
   * 16 bits are ignored.
   */
  def writeShortLE(s: Int): BufWriter

  /**
   * Write 24 bits from `m` in big-endian order. The remaining
   * 8 bits are ignored.
   */
  def writeMediumBE(m: Int): BufWriter

  /**
   * Write 24 bits from `m` in little-endian order. The remaining
   * 8 bits are ignored.
   */
  def writeMediumLE(m: Int): BufWriter

  /**
   * Write 32 bits from `i` in big-endian order.
   */
  def writeIntBE(i: Long): BufWriter

  /**
   * Write 32 bits from `i` in little-endian order.
   */
  def writeIntLE(i: Long): BufWriter

  /**
   * Write 64 bits from `l` in big-endian order.
   */
  def writeLongBE(l: Long): BufWriter

  /**
   * Write 64 bits from `l` in little-endian order.
   */
  def writeLongLE(l: Long): BufWriter

  /**
   * Write 32 bits from `f` in big-endian order.
   */
  def writeFloatBE(f: Float): BufWriter

  /**
   * Write 32 bits from `f` in little-endian order.
   */
  def writeFloatLE(f: Float): BufWriter

  /**
   * Write 64 bits from `d` in big-endian order.
   */
  def writeDoubleBE(d: Double): BufWriter

  /**
   * Write 64 bits from `d` in little-endian order.
   */
  def writeDoubleLE(d: Double): BufWriter

  /**
   * Write all the bytes from `bs` into the running [[Buf]].
   */
  def writeBytes(bs: Array[Byte]): BufWriter

  /**
   * In keeping with [[Buf]] terminology, creates a potential zero-copy [[Buf]]
   * which has a reference to the same region as the [[BufWriter]]. That is, if
   * any methods called on the builder are propagated to the returned [[Buf]].
   * By definition, the builder owns the region it is writing to so this is
   * the natural way to coerce a builder to a [[Buf]].
   */
  def owned(): Buf

  /**
   * Offset in bytes of next write. Visible for testing only.
   */
  private[util] def index: Int
}

private[finagle] object BufWriter {
  /**
   * Creates a fixed size [[BufWriter]].
   */
  def fixed(size: Int): BufWriter = {
    require(size >= 0)
    new FixedBufWriter(new Array[Byte](size))
  }

  /**
   * Creates a [[BufWriter]] that grows as needed.
   * The maximum size is Int.MaxValue - 2 (maximum array size).
   *
   * @param estimatedSize the starting size of the backing buffer, in bytes.
   *                      If no size is given, 256 bytes will be used.
   */
  def dynamic(estimatedSize: Int = 256): BufWriter = {
    require(estimatedSize > 0 && estimatedSize <= DynamicBufWriter.MaxBufferSize)
    new DynamicBufWriter(new Array[Byte](estimatedSize))
  }

  /**
   * Indicates there isn't enough room to write into
   * a [[BufWriter]].
   */
  class OverflowException(msg: String) extends Exception(msg)
}

/**
 * A fixed size [[BufWriter]].
 */
private class FixedBufWriter(arr: Array[Byte], private[util] var index: Int = 0) extends BufWriter {
  import BufWriter.OverflowException

  require(index >= 0)
  require(index <= arr.length)

  /**
   * Exposed to allow `DynamicBufWriter` access to the underlying array of its
   * `FixedBufWriter.` DO NOT USE OUTSIDE OF BUFWRITER!
   */
  private[util] def array: Array[Byte] = arr
  def size: Int = arr.length
  def remaining: Int = arr.length - index

  def writeByte(b: Int): BufWriter = {
    if (remaining < 1) {
      throw new OverflowException("insufficient space to write a byte")
    }
    arr(index) = (b & 0xff).toByte
    index += 1
    this
  }

  def writeShortBE(s: Int): BufWriter = {
    if (remaining < 2) {
      throw new OverflowException("insufficient space to write 2 bytes")
    }
    arr(index)   = ((s >>  8) & 0xff).toByte
    arr(index+1) = ((s      ) & 0xff).toByte
    index += 2
    this
  }

  def writeShortLE(s: Int): BufWriter = {
    if (remaining < 2) {
      throw new OverflowException("insufficient space to write 2 bytes")
    }
    arr(index)   = ((s      ) & 0xff).toByte
    arr(index+1) = ((s >>  8) & 0xff).toByte
    index += 2
    this
  }

  def writeMediumBE(m: Int): BufWriter = {
    if (remaining < 3) {
      throw new OverflowException("insufficient space to write 4 bytes")
    }
    arr(index)   = ((m >> 16) & 0xff).toByte
    arr(index+1) = ((m >>  8) & 0xff).toByte
    arr(index+2) = ((m      ) & 0xff).toByte
    index += 3
    this
  }

  def writeMediumLE(m: Int): BufWriter = {
    if (remaining < 3) {
      throw new OverflowException("insufficient space to write 4 bytes")
    }
    arr(index)   = ((m      ) & 0xff).toByte
    arr(index+1) = ((m >>  8) & 0xff).toByte
    arr(index+2) = ((m >> 16) & 0xff).toByte
    index += 3
    this
  }

  def writeIntBE(i: Long): BufWriter = {
    if (remaining < 4) {
      throw new OverflowException("insufficient space to write 4 bytes")
    }
    arr(index)   = ((i >> 24) & 0xff).toByte
    arr(index+1) = ((i >> 16) & 0xff).toByte
    arr(index+2) = ((i >>  8) & 0xff).toByte
    arr(index+3) = ((i      ) & 0xff).toByte
    index += 4
    this
  }

  def writeIntLE(i: Long): BufWriter = {
    if (remaining < 4) {
      throw new OverflowException("insufficient space to write 4 bytes")
    }
    arr(index)   = ((i      ) & 0xff).toByte
    arr(index+1) = ((i >>  8) & 0xff).toByte
    arr(index+2) = ((i >> 16) & 0xff).toByte
    arr(index+3) = ((i >> 24) & 0xff).toByte
    index += 4
    this
  }


  def writeLongBE(l: Long): BufWriter = {
    if (remaining < 8) {
      throw new OverflowException("insufficient space to write 8 bytes")
    }
    arr(index)   = ((l >> 56) & 0xff).toByte
    arr(index+1) = ((l >> 48) & 0xff).toByte
    arr(index+2) = ((l >> 40) & 0xff).toByte
    arr(index+3) = ((l >> 32) & 0xff).toByte
    arr(index+4) = ((l >> 24) & 0xff).toByte
    arr(index+5) = ((l >> 16) & 0xff).toByte
    arr(index+6) = ((l >>  8) & 0xff).toByte
    arr(index+7) = ((l      ) & 0xff).toByte
    index += 8
    this
  }

  def writeLongLE(l: Long): BufWriter = {
    if (remaining < 8) {
      throw new OverflowException("insufficient space to write 8 bytes")
    }
    arr(index)   = ((l      ) & 0xff).toByte
    arr(index+1) = ((l >>  8) & 0xff).toByte
    arr(index+2) = ((l >> 16) & 0xff).toByte
    arr(index+3) = ((l >> 24) & 0xff).toByte
    arr(index+4) = ((l >> 32) & 0xff).toByte
    arr(index+5) = ((l >> 40) & 0xff).toByte
    arr(index+6) = ((l >> 48) & 0xff).toByte
    arr(index+7) = ((l >> 56) & 0xff).toByte
    index += 8
    this
  }

  def writeFloatBE(f: Float): BufWriter = writeIntBE(JFloat.floatToIntBits(f).toLong)

  def writeFloatLE(f: Float): BufWriter = writeIntLE(JFloat.floatToIntBits(f).toLong)

  def writeDoubleBE(d: Double): BufWriter = writeLongBE(JDouble.doubleToLongBits(d))

  def writeDoubleLE(d: Double): BufWriter = writeLongLE(JDouble.doubleToLongBits(d))

  def writeBytes(bs: Array[Byte]): BufWriter = {
    if (remaining < bs.length) {
      throw new OverflowException(s"insufficient space to write ${bs.length} bytes")
    }
    System.arraycopy(bs, 0, arr, index, bs.length)
    index += bs.length
    this
  }

  val owned: Buf = Buf.ByteArray.Owned(arr)
}

/**
 * A dynamically growing buffer.
 *
 * @note The size of the buffer increases by 50% as necessary to contain the content,
 *       up to a maximum size of Int.MaxValue - 2 (maximum array size; platform-specific).
 */
private object DynamicBufWriter {
  val MaxBufferSize: Int = Int.MaxValue - 2
}

private class DynamicBufWriter(arr: Array[Byte]) extends BufWriter {
  import BufWriter.OverflowException
  import DynamicBufWriter._

  var underlying: FixedBufWriter = new FixedBufWriter(arr)

  /**
   * Offset in bytes of next write. Visible for testing only.
   */
  private[util] def index: Int = underlying.index

  // Create a new underlying buffer if `requiredRemainingBytes` will not
  // fit in the current underlying buffer. The size of the buffer is increased
  // by 50% until current and new bytes will fit. If increasing the size causes
  // the buffer size to overflow, but the contents will still fit in `MaxBufferSize`,
  // the size is scaled back to `requiredSize`.
  private[this] def resizeIfNeeded(requiredRemainingBytes: Int): Unit = {
    if (requiredRemainingBytes > underlying.remaining) {

      var size: Int = underlying.size

      val written = underlying.size - underlying.remaining

      val requiredSize: Int = written + requiredRemainingBytes

      // Check to make sure we won't exceed max buffer size
      if (requiredSize < 0 || requiredSize > MaxBufferSize)
        throw new OverflowException(s"maximum dynamic buffer size is $MaxBufferSize." +
          s" Insufficient space to write $requiredRemainingBytes bytes")

      // Increase size of the buffer by 50% until it can contain `requiredBytes`
      while (requiredSize > size && size > 0) {
        size = (size * 3) / 2 + 1
      }

      // If we've overflowed by increasing the size, scale back to `requiredSize`.
      // We know that we can still fit the current + new bytes, because of
      // the requiredSize check above.
      if (size < 0 || requiredSize > MaxBufferSize)
        size = requiredSize

      // Create a new underlying buffer
      val newArr = new Array[Byte](size)
      System.arraycopy(underlying.array, 0, newArr, 0, written)
      underlying = new FixedBufWriter(newArr, written)
    }
  }

  /**
   * Write 8 bits from `b` in `endianness` order, the remaining
   * 24 bits are ignored.
   */
  def writeByte(b: Int): BufWriter = {
    resizeIfNeeded(1)
    underlying.writeByte(b)
    this
  }

  /**
   * Copies the contents of this writer into a `Buf` of the exact size needed.
   */
  def owned(): Buf = {
    // trim the buffer to the size of the contents
    Buf.ByteArray.Owned(underlying.array, 0, underlying.size - underlying.remaining)
  }

  def writeShortBE(s: Int): BufWriter = {
    resizeIfNeeded(2)
    underlying.writeShortBE(s)
    this
  }

  def writeIntBE(i: Int): BufWriter = {
    resizeIfNeeded(4)
    underlying.writeIntBE(i)
    this
  }

  def writeLongBE(l: Long): BufWriter = {
    resizeIfNeeded(8)
    underlying.writeLongBE(l)
    this
  }

  def writeBytes(bs: Array[Byte]): BufWriter = {
    resizeIfNeeded(bs.length)
    underlying.writeBytes(bs)
    this
  }

  def writeShortLE(s: Int): BufWriter = {
    resizeIfNeeded(2)
    underlying.writeShortLE(s)
    this
  }

  def writeMediumLE(m: Int): BufWriter = {
    resizeIfNeeded(3)
    underlying.writeMediumLE(m)
    this
  }

  def writeIntBE(i: Long): BufWriter = {
    resizeIfNeeded(4)
    underlying.writeIntBE(i)
    this
  }

  def writeFloatBE(f: Float): BufWriter = {
    resizeIfNeeded(4)
    underlying.writeFloatBE(f)
    this
  }

  def writeIntLE(i: Long): BufWriter = {
    resizeIfNeeded(4)
    underlying.writeIntLE(i)
    this
  }

  def writeDoubleBE(d: Double): BufWriter = {
    resizeIfNeeded(8)
    underlying.writeDoubleBE(d)
    this
  }

  def writeFloatLE(f: Float): BufWriter = {
    resizeIfNeeded(4)
    underlying.writeFloatLE(f)
    this
  }

  def writeMediumBE(m: Int): BufWriter = {
    resizeIfNeeded(4)
    underlying.writeMediumBE(m)
    this
  }

  def writeLongLE(l: Long): BufWriter = {
    resizeIfNeeded(8)
    underlying.writeLongLE(l)
    this
  }

  def writeDoubleLE(d: Double): BufWriter = {
    resizeIfNeeded(8)
    underlying.writeDoubleLE(d)
    this
  }
}