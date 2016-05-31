package com.twitter.finagle.util

import com.twitter.io.Buf
import java.lang.{Double => JDouble, Float => JFloat}

/**
 * A [[BufReader]] provides a stateful API to extract bytes from a
 * [[Buf]]. This conveniently allows codec implementations to
 * decode frames, specifically when they need to decode and interpret
 * the bytes as a numeric value. Unless otherwise stated, [[BufReader]]
 * implementations are not thread safe.
 */
private[finagle] trait BufReader {
  /**
   * The remainder of bytes that the reader is capable of reading.
   */
  def remaining: Int

  /**
   * Extract 8 bits and interpret as a signed integer, advancing the byte cursor by 1.
   */
  def readByte(): Int

  /**
   * Extract 8 bits and interpret as an unsigned integer, advancing the byte cursor by 1.
   */
  def readUnsignedByte(): Int

  /**
   * Extract 16 bits and interpret as a big endian integer, advancing the byte cursor by 2.
   */
  def readShortBE(): Int

  /**
   * Extract 16 bits and interpret as a little endian integer, advancing the byte cursor by 2.
   */
  def readShortLE(): Int

  /**
   * Extract 16 bits and interpret as a big endian unsigned integer, advancing the byte cursor by 2.
   */
  def readUnsignedShortBE(): Int

  /**
   * Extract 16 bits and interpret as a little endian unsigned integer, advancing the byte cursor by 2.
   */
  def readUnsignedShortLE(): Int

  /**
   * Extract 24 bits and interpret as a big endian integer, advancing the byte cursor by 3.
   */
  def readMediumBE(): Int

  /**
   * Extract 24 bits and interpret as a little endian integer, advancing the byte cursor by 3.
   */
  def readMediumLE(): Int

  /**
   * Extract 24 bits and interpret as a big endian unsigned integer, advancing the byte cursor by 3.
   */
  def readUnsignedMediumBE(): Int

  /**
   * Extract 24 bits and interpret as a little endian unsigned integer, advancing the byte cursor by 3.
   */
  def readUnsignedMediumLE(): Int

  /**
   * Extract 32 bits and interpret as a big endian integer, advancing the byte cursor by 4.
   */
  def readIntBE(): Int

  /**
   * Extract 32 bits and interpret as a little endian int, advancing the byte cursor by 4.
   */
  def readIntLE(): Int

  /**
   * Extract 32 bits and interpret as a big endian unsigned integer, advancing the byte cursor by 4.
   */
  def readUnsignedIntBE(): Long

  /**
   * Extract 32 bits and interpret as a little endian unsigned integer, advancing the byte cursor by 4.
   */
  def readUnsignedIntLE(): Long

  /**
   * Extract 64 bits and interpret as a big endian integer, advancing the byte cursor by 8.
   */
  def readLongBE(): Long

  /**
   * Extract 64 bits and interpret as a little endian integer, advancing the byte cursor by 8.
   */
  def readLongLE(): Long

  /**
   * Extract 32 bits and interpret as a big endian floating point, advancing the byte cursor by 4.
   */
  def readFloatBE(): Float

  /**
   * Extract 32 bits and interpret as a little endian floating point, advancing the byte cursor by 4.
   */
  def readFloatLE(): Float

  /**
   * Extract 64 bits and interpret as a big endian floating point, advancing the byte cursor by 4.
   */
  def readDoubleBE(): Double

  /**
   * Extract 64 bits and interpret as a little endian floating point, advancing the byte cursor by 4.
   */
  def readDoubleLE(): Double


  /**
   * Returns a new buffer representing a slice of this buffer, delimited
   * by the indices `[cursor, remaining)`. Out of bounds indices are truncated.
   * Negative indices are not accepted.
   */
  def readBytes(n: Int): Buf

  /**
   * Like `read`, but extracts the remainder of bytes from cursor
   * to the length. Note, this advances the cursor to the end of
   * the buf.
   */
  def readAll(): Buf
}

private[finagle] trait ProxyBufReader extends BufReader {
  protected def reader: BufReader

  def remaining: Int = reader.remaining

  def readByte(): Int = reader.readByte()

  def readUnsignedByte(): Int = reader.readUnsignedByte()

  def readShortBE(): Int = reader.readShortBE()

  def readShortLE(): Int = reader.readShortLE()

  def readUnsignedShortBE(): Int = reader.readUnsignedShortBE()

  def readUnsignedShortLE(): Int = reader.readUnsignedShortLE()

  def readMediumBE(): Int = reader.readMediumBE()

  def readMediumLE(): Int = reader.readMediumLE()

  def readUnsignedMediumBE(): Int = reader.readUnsignedMediumBE()

  def readUnsignedMediumLE(): Int = reader.readUnsignedMediumLE()

  def readIntBE(): Int = reader.readIntBE()

  def readIntLE(): Int = reader.readIntLE()

  def readUnsignedIntBE(): Long = reader.readUnsignedIntBE()

  def readUnsignedIntLE(): Long = reader.readUnsignedIntLE()

  def readLongBE(): Long = reader.readLongBE()

  def readLongLE(): Long = reader.readLongLE()

  def readFloatBE(): Float = reader.readFloatBE()

  def readFloatLE(): Float = reader.readFloatLE()

  def readDoubleBE(): Double = reader.readDoubleBE()

  def readDoubleLE(): Double = reader.readDoubleLE()

  def readBytes(n: Int): Buf = reader.readBytes(n)

  def readAll(): Buf = reader.readAll()
}

private[finagle] object BufReader {
  /**
   * Creates a [[BufReader]] that is capable of extracting
   * numeric values in big endian order.
   */
  def apply(buf: Buf): BufReader = new BufReaderImpl(buf)

  /**
   * Indicates there aren't sufficient bytes to be read.
   */
  class UnderflowException(msg: String) extends Exception(msg)

  /**
   * The max value of a signed 24 bit "medium" integer
   */
  private[util] val SignedMediumMax = 0x800000
}

private class BufReaderImpl(underlying: Buf) extends BufReader {
  import BufReader._

  // stores a reference to the remainder `underlying`.
  private[this] var buf: Buf = underlying

  // temporarily stores numeric values which can be extracted
  // from `buf` via the `read*` methods.
  private[this] val nums = new Array[Byte](8)

  def remaining: Int = buf.length

  def readByte(): Int = {
    if (remaining < 1) {
      throw new UnderflowException(
        s"tried to read a byte when remaining bytes was ${remaining}")
    }

    readBytes(1).write(nums, 0)
    nums(0)
  }

  def readUnsignedByte(): Int = readByte() & 0xff

  // - Short -
  def readShortBE(): Int = {
    if (remaining < 2) {
      throw new UnderflowException(
        s"tried to read 2 bytes when remaining bytes was ${remaining}")
    }

    readBytes(2).write(nums, 0)
    ((nums(0) & 0xff) <<  8) |
    ((nums(1) & 0xff)      )
  }

  def readShortLE(): Int = {
    if (remaining < 2) {
      throw new UnderflowException(
        s"tried to read 2 bytes when remaining bytes was ${remaining}")
    }

    readBytes(2).write(nums, 0)
    ((nums(0) & 0xff)      ) |
    ((nums(1) & 0xff) <<  8)
  }

  def readUnsignedShortBE(): Int = readShortBE() & 0xffff

  def readUnsignedShortLE(): Int = readShortLE() & 0xffff

  // - Medium -
  def readMediumBE(): Int = {
    val unsigned = readUnsignedMediumBE()
    if (unsigned > SignedMediumMax) {
      unsigned | 0xff000000
    } else {
      unsigned
    }
  }

  def readMediumLE(): Int = {
    val unsigned = readUnsignedMediumLE()
    if (unsigned > SignedMediumMax) {
      unsigned | 0xff000000
    } else {
      unsigned
    }
  }

  def readUnsignedMediumBE(): Int = {
    if (remaining < 3) {
      throw new UnderflowException(
        s"tried to read 3 bytes when remaining bytes was ${remaining}")
    }

    readBytes(3).write(nums, 0)
    ((nums(0) & 0xff) << 16) |
    ((nums(1) & 0xff) <<  8) |
    ((nums(2) & 0xff)      )
  }

  def readUnsignedMediumLE(): Int = {
    if (remaining < 3) {
      throw new UnderflowException(
        s"tried to read 3 bytes when remaining bytes was ${remaining}")
    }

    readBytes(3).write(nums, 0)
    ((nums(0) & 0xff)      ) |
    ((nums(1) & 0xff) <<  8) |
    ((nums(2) & 0xff) << 16)
  }

  // - Int -
  def readIntBE(): Int = {
    if (remaining < 4) {
      throw new UnderflowException(
        s"tried to read 4 bytes when remaining bytes was ${remaining}")
    }

    readBytes(4).write(nums, 0)
    ((nums(0) & 0xff) << 24) |
    ((nums(1) & 0xff) << 16) |
    ((nums(2) & 0xff) <<  8) |
    ((nums(3) & 0xff)      )
  }

  def readIntLE(): Int = {
    if (remaining < 4) {
      throw new UnderflowException(
        s"tried to read 4 bytes when remaining bytes was ${remaining}")
    }

    readBytes(4).write(nums, 0)
    ((nums(0) & 0xff)      ) |
    ((nums(1) & 0xff) <<  8) |
    ((nums(2) & 0xff) << 16) |
    ((nums(3) & 0xff) << 24)
  }

  def readUnsignedIntBE(): Long = readIntBE() & 0xffffffffL

  def readUnsignedIntLE(): Long = readIntLE() & 0xffffffffL

  // - Long -
  def readLongBE(): Long = {
    if (remaining < 8) {
      throw new UnderflowException(
        s"tried to read 8 bytes when remaining bytes was ${remaining}")
    }

    readBytes(8).write(nums, 0)
    ((nums(0) & 0xff).toLong << 56) |
    ((nums(1) & 0xff).toLong << 48) |
    ((nums(2) & 0xff).toLong << 40) |
    ((nums(3) & 0xff).toLong << 32) |
    ((nums(4) & 0xff).toLong << 24) |
    ((nums(5) & 0xff).toLong << 16) |
    ((nums(6) & 0xff).toLong <<  8) |
    ((nums(7) & 0xff).toLong      )
  }

  def readLongLE(): Long = {
    if (remaining < 8) {
      throw new UnderflowException(
        s"tried to read 8 bytes when remaining bytes was ${remaining}")
    }

    readBytes(8).write(nums, 0)
    ((nums(0) & 0xff).toLong      ) |
    ((nums(1) & 0xff).toLong <<  8) |
    ((nums(2) & 0xff).toLong << 16) |
    ((nums(3) & 0xff).toLong << 24) |
    ((nums(4) & 0xff).toLong << 32) |
    ((nums(5) & 0xff).toLong << 40) |
    ((nums(6) & 0xff).toLong << 48) |
    ((nums(7) & 0xff).toLong << 56)
  }

  // - Floating Point -
  def readFloatBE(): Float = JFloat.intBitsToFloat(readIntBE())

  def readDoubleBE(): Double = JDouble.longBitsToDouble(readLongBE())

  def readFloatLE(): Float = JFloat.intBitsToFloat(readIntLE())

  def readDoubleLE(): Double = JDouble.longBitsToDouble(readLongLE())

  def readBytes(n: Int): Buf = {
    val b = buf.slice(0, n)
    buf = buf.slice(n, buf.length)
    b
  }

  def readAll(): Buf = {
    val b = buf.slice(0, buf.length)
    buf = Buf.Empty
    b
  }
}