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
   * The remainder of bytes until the first occurrence of `byte` (exclusive).
   * Returns `-1` when `byte` is not found on the underlying [[Buf]].
   *
   * @note All util and finagle Buf implementations will not copy onto
   *       the heap, but for other implementations, it may trigger a copy.
   */
  def remainingUntil(byte: Byte): Int

  /**
   * Extract 8 bits and interpret as a signed integer, advancing the byte cursor by 1.
   */
  def readByte(): Byte

  /**
   * Extract 8 bits and interpret as an unsigned integer, advancing the byte cursor by 1.
   */
  def readUnsignedByte(): Short

  /**
   * Extract 16 bits and interpret as a big endian integer, advancing the byte cursor by 2.
   */
  def readShortBE(): Short

  /**
   * Extract 16 bits and interpret as a little endian integer, advancing the byte cursor by 2.
   */
  def readShortLE(): Short

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
   * Skip over the next `n` bytes.
   *
   * @throws UnderflowException if there are < `n` bytes available
   */
  def skip(n: Int): Unit

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

  def remainingUntil(byte: Byte): Int = reader.remainingUntil(byte)

  def readByte(): Byte = reader.readByte()

  def readUnsignedByte(): Short = reader.readUnsignedByte()

  def readShortBE(): Short = reader.readShortBE()

  def readShortLE(): Short = reader.readShortLE()

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

  def skip(n: Int): Unit = reader.skip(n)

  def readAll(): Buf = reader.readAll()
}

private[finagle] object BufReader {
  /**
   * Creates a [[BufReader]].
   */
  def apply(buf: Buf): BufReader =
    new BufReaderImpl(buf)

  /**
   * Indicates there aren't sufficient bytes to be read.
   */
  class UnderflowException(msg: String) extends Exception(msg)

  /**
   * The max value of a signed 24 bit "medium" integer
   */
  private[util] val SignedMediumMax = 0x800000
}

private class BufReaderImpl(buf: Buf) extends BufReader {
  import BufReader._

  private[this] val len = buf.length

  private[this] var pos = 0

  def remaining: Int = len - pos

  private[this] def checkRemaining(needed: Int): Unit =
    if (remaining < needed) {
      throw new UnderflowException(
        s"tried to read $needed byte(s) when remaining bytes was $remaining")
    }

  private[this] def byteFinder(target: Byte): Buf.Processor =
    new Buf.Processor {
      def apply(byte: Byte): Boolean = byte != target
    }

  // Time - O(n), Memory - O(1)
  def remainingUntil(byte: Byte): Int =
    if (remaining == 0) -1
    else {
      val index = buf.process(pos, len, byteFinder(byte))
      if (index == -1) -1
      else index - pos
    }

  def readByte(): Byte = {
    checkRemaining(1)
    val ret = buf.get(pos)
    pos += 1
    ret
  }

  def readUnsignedByte(): Short = (readByte() & 0xff).toShort

  // - Short -
  def readShortBE(): Short = {
    checkRemaining(2)
    val ret =
      (buf.get(pos    ) & 0xff) << 8 |
      (buf.get(pos + 1) & 0xff)
    pos += 2
    ret.toShort
  }

  def readShortLE(): Short = {
    checkRemaining(2)
    val ret =
      (buf.get(pos    ) & 0xff) |
      (buf.get(pos + 1) & 0xff) << 8
    pos += 2
    ret.toShort
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
    checkRemaining(3)
    val ret =
      (buf.get(pos    ) & 0xff) << 16 |
      (buf.get(pos + 1) & 0xff) <<  8 |
      (buf.get(pos + 2) & 0xff)
    pos += 3
    ret
  }

  def readUnsignedMediumLE(): Int = {
    checkRemaining(3)
    val ret =
      (buf.get(pos    ) & 0xff)       |
      (buf.get(pos + 1) & 0xff) <<  8 |
      (buf.get(pos + 2) & 0xff) << 16
    pos += 3
    ret
  }

  // - Int -
  def readIntBE(): Int = {
    checkRemaining(4)
    val ret =
      (buf.get(pos    ) & 0xff) << 24 |
      (buf.get(pos + 1) & 0xff) << 16 |
      (buf.get(pos + 2) & 0xff) <<  8 |
      (buf.get(pos + 3) & 0xff)
    pos += 4
    ret
  }

  def readIntLE(): Int = {
    checkRemaining(4)
    val ret =
      (buf.get(pos    ) & 0xff)       |
      (buf.get(pos + 1) & 0xff) <<  8 |
      (buf.get(pos + 2) & 0xff) << 16 |
      (buf.get(pos + 3) & 0xff) << 24
    pos += 4
    ret
  }

  def readUnsignedIntBE(): Long = readIntBE() & 0xffffffffL

  def readUnsignedIntLE(): Long = readIntLE() & 0xffffffffL

  // - Long -
  def readLongBE(): Long = {
    checkRemaining(8)
    val ret =
      (buf.get(pos    ) & 0xff).toLong << 56 |
      (buf.get(pos + 1) & 0xff).toLong << 48 |
      (buf.get(pos + 2) & 0xff).toLong << 40 |
      (buf.get(pos + 3) & 0xff).toLong << 32 |
      (buf.get(pos + 4) & 0xff).toLong << 24 |
      (buf.get(pos + 5) & 0xff).toLong << 16 |
      (buf.get(pos + 6) & 0xff).toLong <<  8 |
      (buf.get(pos + 7) & 0xff).toLong
    pos += 8
    ret
  }

  def readLongLE(): Long = {
    checkRemaining(8)
    val ret =
      (buf.get(pos    ) & 0xff).toLong       |
      (buf.get(pos + 1) & 0xff).toLong <<  8 |
      (buf.get(pos + 2) & 0xff).toLong << 16 |
      (buf.get(pos + 3) & 0xff).toLong << 24 |
      (buf.get(pos + 4) & 0xff).toLong << 32 |
      (buf.get(pos + 5) & 0xff).toLong << 40 |
      (buf.get(pos + 6) & 0xff).toLong << 48 |
      (buf.get(pos + 7) & 0xff).toLong << 56
    pos += 8
    ret
  }

  // - Floating Point -
  def readFloatBE(): Float = JFloat.intBitsToFloat(readIntBE())

  def readDoubleBE(): Double = JDouble.longBitsToDouble(readLongBE())

  def readFloatLE(): Float = JFloat.intBitsToFloat(readIntLE())

  def readDoubleLE(): Double = JDouble.longBitsToDouble(readLongLE())

  def readBytes(n: Int): Buf = {
    if (n < 0) {
      throw new IllegalArgumentException(s"'n' must be non-negative: $n")
    } else {
      val ret = buf.slice(pos, pos + n)
      pos += n
      ret
    }
  }

  def skip(n: Int): Unit = {
    if (n < 0) {
      throw new IllegalArgumentException(s"'n' must be non-negative: $n")
    }
    checkRemaining(n)
    pos += n
  }

  def readAll(): Buf = {
    val ret = buf.slice(pos, len)
    pos = len
    ret
  }
}
