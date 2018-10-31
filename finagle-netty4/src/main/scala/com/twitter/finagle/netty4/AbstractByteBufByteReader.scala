package com.twitter.finagle.netty4

import com.twitter.io.{Buf, ByteReader}
import com.twitter.io.ByteReader.UnderflowException
import java.lang.{Double => JDouble, Float => JFloat}
import io.netty.buffer.ByteBuf
import io.netty.util.ByteProcessor
import java.nio.charset.Charset

/**
 * Abstract implementation of `ByteReader` that wraps the Netty 4 `ByteBuf` type.
 *
 * Operations on this `ByteReader` may mutate the reader index of the underlying `ByteBuf`, but
 * will not mutate the data or writer index.
 *
 * @note This `ByteReader` and its concrete implementations are not thread safe.
 *
 * @note This type doesn't assume an ownership interest in the passed `ByteBuf`, but concrete
 *       implementations may. Eg, this construct does not manipulate the reference count of the
 *       wrapped `ByteBuf` but a concrete implementation may retain slices of the underlying
 *       `ByteBuf`.
 *
 * @see [[CopyingByteBufByteReader]] for a concrete implementation that creates `Buf` instances
 *      via an explicit copy to the heap.
 */
private[netty4] abstract class AbstractByteBufByteReader(bb: ByteBuf) extends ByteReader {

  private[this] var closed: Boolean = false

  final def remaining: Int = bb.readableBytes()

  final def remainingUntil(byte: Byte): Int = bb.bytesBefore(byte)

  def process(processor: Buf.Processor): Int =
    process(0, bb.readableBytes(), processor)

  def process(from: Int, until: Int, processor: Buf.Processor): Int =
    AbstractByteBufByteReader.process(from, until, processor, bb)

  final def readString(numBytes: Int, charset: Charset): String = {
    checkRemaining(numBytes)
    val result = bb.toString(bb.readerIndex, numBytes, charset)
    bb.readerIndex(bb.readerIndex + numBytes)
    result
  }

  final def readByte(): Byte = {
    checkRemaining(1)
    bb.readByte()
  }

  final def readUnsignedByte(): Short = {
    checkRemaining(1)
    bb.readUnsignedByte()
  }

  final def readShortBE(): Short = {
    checkRemaining(2)
    bb.readShort()
  }

  final def readShortLE(): Short = {
    checkRemaining(2)
    bb.readShortLE()
  }

  final def readUnsignedShortBE(): Int = {
    checkRemaining(2)
    bb.readUnsignedShort()
  }

  final def readUnsignedShortLE(): Int = {
    checkRemaining(2)
    bb.readUnsignedShortLE()
  }

  final def readMediumBE(): Int = {
    checkRemaining(3)
    bb.readMedium()
  }

  final def readMediumLE(): Int = {
    checkRemaining(3)
    bb.readMediumLE()
  }

  final def readUnsignedMediumBE(): Int = {
    checkRemaining(3)
    bb.readUnsignedMedium()
  }

  final def readUnsignedMediumLE(): Int = {
    checkRemaining(3)
    bb.readUnsignedMediumLE()
  }

  final def readIntBE(): Int = {
    checkRemaining(4)
    bb.readInt()
  }

  final def readIntLE(): Int = {
    checkRemaining(4)
    bb.readIntLE()
  }

  final def readUnsignedIntBE(): Long = {
    checkRemaining(4)
    bb.readUnsignedInt()
  }

  final def readUnsignedIntLE(): Long = {
    checkRemaining(4)
    bb.readUnsignedIntLE()
  }

  final def readLongBE(): Long = {
    checkRemaining(8)
    bb.readLong()
  }

  final def readLongLE(): Long = {
    checkRemaining(8)
    bb.readLongLE()
  }

  final def readUnsignedLongBE(): BigInt = {
    checkRemaining(8)

    // 9 (8+1) so sign bit is always positive
    val bytes: Array[Byte] = new Array[Byte](9)
    bytes(0) = 0
    bytes(1) = bb.readByte()
    bytes(2) = bb.readByte()
    bytes(3) = bb.readByte()
    bytes(4) = bb.readByte()
    bytes(5) = bb.readByte()
    bytes(6) = bb.readByte()
    bytes(7) = bb.readByte()
    bytes(8) = bb.readByte()

    BigInt(bytes)
  }

  final def readUnsignedLongLE(): BigInt = {
    checkRemaining(8)

    // 9 (8+1) so sign bit is always positive
    val bytes: Array[Byte] = new Array[Byte](9)
    bytes(8) = bb.readByte()
    bytes(7) = bb.readByte()
    bytes(6) = bb.readByte()
    bytes(5) = bb.readByte()
    bytes(4) = bb.readByte()
    bytes(3) = bb.readByte()
    bytes(2) = bb.readByte()
    bytes(1) = bb.readByte()
    bytes(0) = 0

    BigInt(bytes)
  }

  final def readFloatBE(): Float = {
    JFloat.intBitsToFloat(readIntBE())
  }

  final def readFloatLE(): Float = {
    JFloat.intBitsToFloat(readIntLE())
  }

  final def readDoubleBE(): Double = {
    JDouble.longBitsToDouble(readLongBE())
  }

  final def readDoubleLE(): Double = {
    JDouble.longBitsToDouble(readLongLE())
  }

  final def skip(n: Int): Unit = {
    if (n < 0) {
      throw new IllegalArgumentException(s"'n' must be non-negative: $n")
    }
    checkRemaining(n)
    bb.skipBytes(n)
  }

  final def readAll(): Buf = readBytes(remaining)

  final def close(): Unit = {
    if (!closed) {
      closed = true
      bb.release()
    }
  }

  final protected def checkRemaining(needed: Int): Unit =
    if (remaining < needed) {
      throw new UnderflowException(
        s"tried to read $needed byte(s) when remaining bytes was $remaining"
      )
    }
}

private object AbstractByteBufByteReader {

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
  private def process(from: Int, until: Int, processor: Buf.Processor, bb: ByteBuf): Int = {
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
}
