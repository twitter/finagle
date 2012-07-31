package com.twitter.finagle.mysql.protocol

import java.lang.{Float => JFloat, Double => JDouble}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
 * BufferReader and BufferWriter provide operations to read/write
 * to a Array[Byte] in little-endian byte order. All the operations
 * are side-effecting. That is, a call to a method will advance
 * the current position (offset) in the buffer.
 * 
 * The MySQL Client/Server protocol represents all data in little endian byte order.
 */

case object CorruptBufferException
  extends Exception("Corrupt data or client/server are out of sync.")

trait BufferReader {
  /**
   * Denotes if the buffer is readable upto the given width
   * based on the current offset.
   */
  def readable(width: Int): Boolean

  /**
   * Reads multi-byte numeric values stored in a byte array. 
   * The values are assumed to be stored as little-endian and the result 
   * is returned as a Long. If you are looking to read longer numeric values,
   * use the take(n) method.
   */
  def read(width: Int): Long

  def readByte() = read(1).toByte
  def readUnsignedByte() = read(1).toShort
  def readShort() = read(2).toShort
  def readUnsignedShort() = read(2).toInt
  def readInt24() = read(3).toInt
  def readInt() = read(4).toInt
  def readLong() = read(8)
  def readFloat() = JFloat.intBitsToFloat(readInt)
  def readDouble() = JDouble.longBitsToDouble(readLong)

  def skip(n: Int): Unit

  /**
   * Consumes the rest of the buffer.
   * @return Array[Byte] containing the rest of the buffer.
   */
  def takeRest(): Array[Byte]

  /**
   * Consumes n bytes in the buffer.
   * @return Array[Byte] containing bytes from offset to offset+n
   */
  def take(n: Int): Array[Byte]

  /**
   * Read MySQL data field - a variable-length number.
   * Depending on the first byte, read a different width from
   * the buffer. For more info, refer to MySQL Client/Server protocol
   * documentation.
   */
  def readLengthCodedBinary(): Long = {
    val firstByte = readUnsignedByte()
    if (firstByte < 251)
      firstByte
    else
      firstByte match {
        case 251 => BufferReader.NULL_LENGTH
        case 252 => read(2)
        case 253 => read(3)
        case 254 => read(8)
        case _ => 
          throw CorruptBufferException
      }
  }

  /**
   * Reads a null terminated string in the given charset, where
   * null is denoted by '\0'. Uses Charset.defaultCharset
   * to decode strings.
   */ 
  def readNullTerminatedString(): String

  /**
   * Reads a length encoded string according to the MySQL
   * Client/Server protocol. For more details refer to MySQL
   * documentation. Uses Charset.defaultCharset to 
   * decode strings.
   */
  def readLengthCodedString(): String = {
    val len = readLengthCodedBinary().toInt
    if (len == BufferReader.NULL_LENGTH)
       null
    else if (len == 0)
      BufferReader.EMPTY_STRING
    else
      new String(take(len), Charset.defaultCharset)
  }

  /**
   * Reads a length encoded set of bytes according to the MySQL
   * Client/Server protocol. For more details refer to MySQL
   * documentation.
   */
  def readLengthCodedBytes(): Array[Byte] = {
    val len = readLengthCodedBinary().toInt
    if (len == BufferReader.NULL_LENGTH)
      null
    else if (len == 0)
      BufferReader.EMPTY_BYTE_ARRAY
    else
      take(len)
  }

  /**
   * Returns the underlying Array[Byte].
   */
  def buffer: Array[Byte]

  /**
   * Wraps the underlying buffer into a 
   * netty buffer. Note, netty wrappedBuffers
   * avoid copying.
   */
  def toChannelBuffer: ChannelBuffer = ChannelBuffers.wrappedBuffer(buffer)
}

object BufferReader {
  val NULL_LENGTH = -1 // denotes a SQL NULL value when reading a length coded binary.
  val EMPTY_STRING = new String
  val EMPTY_BYTE_ARRAY = new Array[Byte](0)

  def apply(underlying: Array[Byte], startOffset: Int = 0): BufferReader =
    new DefaultBufferReader(underlying, startOffset)
}

class DefaultBufferReader(val buffer: Array[Byte], var offset: Int) extends BufferReader {
  require(offset >= 0)
  require(buffer != null)

  def readable(width: Int = 1): Boolean = offset + width <= buffer.size

  def read(width: Int): Long = {
    val n = (offset until offset + width).zipWithIndex.foldLeft(0L) {
      case (result, (b,i)) => result | ((buffer(b) & 0xFFL) << (i*8))
    }
    offset += width
    n
  }

  def skip(n: Int) = offset += n

  def takeRest() = take(buffer.size - offset)

  def take(n: Int) = {
    val res = new Array[Byte](n)
    Array.copy(buffer, offset, res, 0, n)
    offset += n
    res
  }

  def readNullTerminatedString(): String = {
    val start = offset
    var length = 0

    while(buffer(offset) != 0) {
      offset += 1
      length += 1
    }

    offset += 1 // consume null byte
    new String(buffer, start, length, Charset.defaultCharset)
  }
}

trait BufferWriter {
  /**
   * Denotes if the buffer is writable upto the given width
   * based on the current offset.
   */
  def writable(width: Int): Boolean

  /**
   * Write multi-byte numeric values onto the the buffer in little-endian
   * byte order. Only supports writing upto 8 bytes (Long). 
   * For larger numeric values use the writeBytes method.
   * @param n Numeric value to write onto the buffer.
   * @param width The number of bytes used to widen n.
   */
  def write(n: Long, width: Int): BufferWriter

  def writeBoolean(b: Boolean) = if (b) writeByte(0) else writeByte(1)
  def writeByte(n: Byte) = write(n, 1)
  def writeUnsignedByte(n: Int) = write(n, 1)
  def writeShort(n: Short) = write(n, 2)
  def writeUnsignedShort(n: Int) = write(n, 2)
  def writeInt24(n: Int) = write(n,  3)
  def writeInt(n: Int) = write(n, 4)
  def writeLong(n: Long) = write(n, 8)
  def writeFloat(f: Float) = writeInt(JFloat.floatToIntBits(f))
  def writeDouble(d: Double) = writeLong(JDouble.doubleToLongBits(d))

  def skip(n: Int): BufferWriter

  /**
   * Fills the rest of the buffer with the given byte.
   * @param b Byte used to fill.
   */
  def fillRest(b: Byte): BufferWriter

  /**
   * Fills the buffer from current offset to offset+n with b.
   * @param n width to fill
   * @param b Byte used to fill.
   */
  def fill(n: Int, b: Byte): BufferWriter

  /**
   * Writes bytes onto the buffer.
   * @param bytes Array[Byte] to copy onto the buffer.
   */
   def writeBytes(bytes: Array[Byte]): BufferWriter

   /**
    * Writes a length coded binary according the the MySQL
    * Client/Server protocol. Refer to MySQL documentation for
    * more information.
    */
   def writeLengthCodedBinary(length: Long): BufferWriter = {
     if (length < 251) {
        write(length, 1)
      } else if (length < 65536) {
        write(252, 1)
        write(length, 2)
      } else if (length < 16777216) {
        write(253, 1)
        write(length, 3)
      } else {
        write(254, 1)
        write(length, 8)
      }
    }

   /**
    * Writes a null terminated string onto the buffer where
    * '\0' denotes null. Note, uses Charset.defaultCharset to decode string.
    * @param s String to write.
    */
   def writeNullTerminatedString(s: String): BufferWriter = {
    writeBytes(s.getBytes(Charset.defaultCharset))
    writeByte('\0'.toByte)
    this
   }

   /**
    * Writes a length coded string using the MySQL Client/Server
    * protocol. Note, uses Charset.defaultCharset to decode string.
    * @param s String to write to buffer.
    */
   def writeLengthCodedString(s: String): BufferWriter = {
    writeLengthCodedBinary(s.length)
    writeBytes(s.getBytes(Charset.defaultCharset))
    this
   }

   def writeLengthCodedString(strAsBytes: Array[Byte]): BufferWriter = {
    writeLengthCodedBinary(strAsBytes.length)
    writeBytes(strAsBytes)
    this
  }

  /**
   * Returns the underlying Array[Byte].
   */
  def buffer: Array[Byte]

  /**
   * Wraps the underlying buffer into a 
   * netty buffer. Note, netty wrappedBuffers
   * avoid copying.
   */
  def toChannelBuffer: ChannelBuffer = ChannelBuffers.wrappedBuffer(buffer)
}

object BufferWriter {

  def apply(underlying: Array[Byte], startOffset: Int = 0): BufferWriter =
    new DefaultBufferWriter(underlying, startOffset)
}

class DefaultBufferWriter(val buffer: Array[Byte], private[this] var offset: Int) extends BufferWriter {
  require(offset >= 0)
  require(buffer != null)

  def writable(width: Int = 1): Boolean = offset + width <= buffer.size

  def write(n: Long, width: Int): BufferWriter = {
    (0 until width) foreach { i =>
      buffer(i + offset) = ((n >> (i*8)) & 0xFF).toByte
    }
    offset += width
    this
  }

  def skip(n: Int) = {
    offset += n
    this
  }

  def fillRest(b: Byte) = fill(buffer.size - offset, b)

  def fill(n: Int, b: Byte) = {
    (offset until offset + n) foreach { j => buffer(j) = b ; offset += 1 }
    this
  }

  def writeBytes(bytes: Array[Byte]) = {
    Array.copy(bytes, 0, buffer, offset, bytes.length)
    offset += bytes.length
    this
  }
}
