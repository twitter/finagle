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
}

private[finagle] object BufWriter {
  /**
   * Creates a big endian, fixed size [[BufWriter]].
   */
  def fixed(size: Int): BufWriter = {
    require(size >= 0)
    new FixedBufWriter(new Array[Byte](size))
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
private class FixedBufWriter(arr: Array[Byte]) extends BufWriter {
  import BufWriter.OverflowException

  private[this] var index: Int = 0

  private[this] def remaining = arr.length - index

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
    index += 4
    this
  }

  def writeMediumLE(m: Int): BufWriter = {
    if (remaining < 3) {
      throw new OverflowException("insufficient space to write 4 bytes")
    }
    arr(index)   = ((m      ) & 0xff).toByte
    arr(index+1) = ((m >>  8) & 0xff).toByte
    arr(index+2) = ((m >> 16) & 0xff).toByte
    index += 4
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