package com.twitter.finagle.netty4

import com.twitter.io.{Buf, ByteReader}
import com.twitter.io.ByteReader.UnderflowException
import java.lang.{Double => JDouble, Float => JFloat}
import io.netty.buffer.ByteBuf

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
        s"tried to read $needed byte(s) when remaining bytes was $remaining")
    }
}
