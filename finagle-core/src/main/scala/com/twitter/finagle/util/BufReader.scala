package com.twitter.finagle.util

import com.twitter.io.Buf
import java.nio.ByteOrder

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
   * Extract 8 bits, advancing the byte cursor by 1.
   */
  def readByte(): Int

  /**
   * Extract 16 bits, advancing the byte cursor by 2.
   */
  def readShortBE(): Int

  /**
   * Extract 32 bits, advancing the byte cursor by 4.
   */
  def readIntBE(): Int

  /**
   * Extract 64 bits, advancing the byte cursor by 8.
   */
  def readLongBE(): Long

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
}

private class BufReaderImpl(underlying: Buf) extends BufReader {
  import BufReader.UnderflowException

  // stores a reference to the remainder `underlying`.
  private[this] var buf: Buf = underlying

  // temporarily stores numeric values which can be extracted
  // from `buf` via the `read*` methods.
  private[this] val nums = new Array[Byte](8)

  def remaining = buf.length

  def readByte(): Int = {
    if (remaining < 1) {
      throw new UnderflowException(
        s"tried to read a byte when remaining bytes=${remaining}")
    }

    readBytes(1).write(nums, 0)
    nums(0)
  }

  def readShortBE(): Int = {
    if (remaining < 2) {
      throw new UnderflowException(
        s"tried to read 2 bytes when remaining bytes=${remaining}")
    }

    readBytes(2).write(nums, 0)
    ((nums(0) & 0xff) <<  8) |
    ((nums(1) & 0xff)      )
  }

  def readIntBE(): Int = {
    if (remaining < 4) {
      throw new UnderflowException(
        s"tried to read 4 bytes when remaining bytes=${remaining}")
    }

    readBytes(4).write(nums, 0)
    ((nums(0) & 0xff) << 24) |
    ((nums(1) & 0xff) << 16) |
    ((nums(2) & 0xff) <<  8) |
    ((nums(3) & 0xff)      )
  }

  def readLongBE(): Long = {
    if (remaining < 8) {
      throw new UnderflowException(
        s"tried to read 8 bytes when remaining bytes=${remaining}")
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