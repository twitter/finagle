package com.twitter.finagle.mysql.protocol

import java.lang.{Float => JFloat, Double => JDouble}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._
import java.nio.ByteOrder

/**
 * BufferReader and BufferWriter provide operations to read/write
 * to a Array[Byte] in little-endian byte order. All the operations
 * are side-effecting. That is, a call to a method will advance
 * the current reader/writer position (offset) in the buffer.
 * 
 * The MySQL Client/Server protocol represents all data in little endian byte order.
 */

case object CorruptBufferException
  extends Exception("Corrupt data or client/server are out of sync.")

trait BufferReader {
  /**
   * Current reader offset in the buffer.
   */
  def offset: Int

  /**
   * Denotes if the buffer is readable upto the given width
   * based on the current offset.
   */
  def readable(width: Int): Boolean

  /**
   * Returns the current byte at buffer(offset)
   */
  def getByte: Byte

  def readByte(): Byte
  def readUnsignedByte(): Short
  def readShort(): Short
  def readUnsignedShort(): Int
  def readInt24(): Int
  def readUnsignedInt24(): Int
  def readInt(): Int
  def readUnsignedInt(): Long
  def readLong(): Long
  def readFloat(): Float
  def readDouble(): Double

  /**
   * Increases offset by n.
   */
  def skip(n: Int): Unit

  /**
   * Consumes the rest of the buffer and copies
   * it to a new Array.
   * @return Array[Byte] containing the rest of the buffer.
   */
  def takeRest(): Array[Byte]

  /**
   * Consumes n bytes in the buffer and 
   * copies them to a new Array.
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
        case 252 => readUnsignedShort().toLong
        case 253 => readInt24().toLong
        case 254 => readLong()
        case _ => 
          throw CorruptBufferException
      }
  }

  /**
   * Reads a null terminated string in the given charset, where
   * null is denoted by '\0'. Uses Charset.defaultCharset
   * to decode strings.
   */ 
  def readNullTerminatedString(): String = {
    val start = offset
    var length = 0

    while(getByte != 0) {
      skip(1)
      length += 1
    }

    skip(1) // consume null byte
    this.toChannelBuffer.toString(start, length, Charset.defaultCharset)
  }

  /**
   * Reads a length encoded string according to the MySQL
   * Client/Server protocol. For more details refer to MySQL
   * documentation. Uses Charset.defaultCharset to 
   * decode strings.
   */
  def readLengthCodedString(): String = {
    val length = readLengthCodedBinary().toInt
    if (length == BufferReader.NULL_LENGTH)
       null
    else if (length == 0)
      BufferReader.EMPTY_STRING
    else {
      val str = this.toChannelBuffer.toString(offset, length, Charset.defaultCharset)
      skip(length)
      str
    }
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
   * Returns a Netty ChannelBuffer representing
   * the underlying buffer. The ChannelBuffer
   * has ByteOrder.LITTLE_ENDIAN.
   */
  def toChannelBuffer: ChannelBuffer
}

object BufferReader {
  val NULL_LENGTH = -1 // denotes a SQL NULL value when reading a length coded binary.
  val EMPTY_STRING = new String
  val EMPTY_BYTE_ARRAY = new Array[Byte](0)

  /**
   * Creates a BufferReader from an Array[Byte].
   * @param bytes Byte array to read from.
   * @param startOffset initial offset.
   */
  def apply(bytes: Array[Byte], startOffset: Int = 0): BufferReader = {
    require(bytes != null)
    require(startOffset >= 0)

    val underlying = wrappedBuffer(ByteOrder.LITTLE_ENDIAN, bytes)
    underlying.readerIndex(startOffset)
    new WrappedChannelBufferReader(underlying)
  }

  /**
   * Creates a BufferReader from a Netty ChannelBuffer.
   */
  def apply(underlying: ChannelBuffer): BufferReader = {
    require(underlying.order == ByteOrder.LITTLE_ENDIAN)
    new WrappedChannelBufferReader(underlying)
  }
}

/**
 * BufferReader backed by a Netty ChannelBuffer.
 */
class WrappedChannelBufferReader(underlying: ChannelBuffer) extends BufferReader {

  def readable(width: Int = 1): Boolean = underlying.readableBytes == width

  def offset = underlying.readerIndex

  def getByte = underlying.getByte(offset)

  def readByte(): Byte = underlying.readByte()
  def readUnsignedByte(): Short = underlying.readUnsignedByte()
  def readShort(): Short = underlying.readShort()
  def readUnsignedShort(): Int = underlying.readUnsignedShort()
  def readInt24(): Int = underlying.readMedium()
  def readUnsignedInt24(): Int = underlying.readUnsignedMedium()
  def readInt(): Int = underlying.readInt()
  def readUnsignedInt(): Long = underlying.readUnsignedInt()
  def readLong(): Long = underlying.readLong()
  def readFloat() = underlying.readFloat()
  def readDouble() = underlying.readDouble()

  def skip(n: Int) = underlying.skipBytes(n)

  def takeRest() = take(underlying.capacity - offset)

  def take(n: Int) = {
    val res = new Array[Byte](n)
    underlying.readBytes(res)
    res
  }

  def buffer = underlying.array
  def toChannelBuffer = underlying
}

trait BufferWriter {
  /**
   * Current writer offset.
   */
  def offset: Int

  /**
   * Denotes if the buffer is writable upto the given width
   * based on the current offset.
   */
  def writable(width: Int): Boolean

  def writeBoolean(b: Boolean): BufferWriter
  def writeByte(n: Int): BufferWriter
  def writeShort(n: Int): BufferWriter
  def writeInt24(n: Int): BufferWriter
  def writeInt(n: Int): BufferWriter
  def writeLong(n: Long): BufferWriter
  def writeFloat(f: Float): BufferWriter
  def writeDouble(d: Double): BufferWriter

  def skip(n: Int): BufferWriter

  /**
   * Fills the rest of the buffer with the given byte.
   * @param b Byte used to fill.
   */
  def fillRest(b: Byte) = fill(buffer.size - offset, b)

  /**
   * Fills the buffer from current offset to offset+n with b.
   * @param n width to fill
   * @param b Byte used to fill.
   */
  def fill(n: Int, b: Byte) = {
    (offset until offset + n) foreach { j => writeByte(b) }
    this
  }

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
        writeByte(length.toInt)
      } else if (length < 65536) {
        writeByte(252)
        writeShort(length.toInt)
      } else if (length < 16777216) {
        writeByte(253)
        writeInt24(length.toInt)
      } else {
        writeByte(254)
        writeLong(length)
      }
    }

   /**
    * Writes a null terminated string onto the buffer where
    * '\0' denotes null. Note, uses Charset.defaultCharset to decode string.
    * @param s String to write.
    */
   def writeNullTerminatedString(s: String): BufferWriter = {
    writeBytes(s.getBytes(Charset.defaultCharset))
    writeByte('\0')
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
   * Returns a Netty ChannelBuffer representing
   * the underlying buffer. The ChannelBuffer
   * has ByteOrder.LITTLE_ENDIAN.
   */
  def toChannelBuffer: ChannelBuffer
}

object BufferWriter {
  /**
   * Creates a BufferWriter from an Array[Byte].
   * @param bytes Byte array to read from.
   * @param startOffset initial offset.
   */
  def apply(bytes: Array[Byte], startOffset: Int = 0): BufferWriter = {
    require(bytes != null)
    require(startOffset >= 0)

    val underlying = wrappedBuffer(ByteOrder.LITTLE_ENDIAN, bytes)
    underlying.writerIndex(startOffset)
    new WrappedChannelBufferWriter(underlying)
  }

  /**
   * Creates a BufferWriter from a Netty ChannelBuffer.
   */
  def apply(underlying: ChannelBuffer): BufferWriter = {
    require(underlying.order == ByteOrder.LITTLE_ENDIAN)
    new WrappedChannelBufferWriter(underlying)
  }

  /**
   * Calculates the size required to store the length
   * according to the MySQL protocol for length coded
   * binary.
   */
  def sizeOfLen(l: Long) = 
    if (l < 251) 1 else if (l < 65536) 3 else if (l < 16777216) 4 else 9
}

/**
 * BufferWriter backed by a Netty ChannelBuffer.
 */
class WrappedChannelBufferWriter(underlying: ChannelBuffer) extends BufferWriter {

  def offset = underlying.writerIndex

  def writable(width: Int = 1): Boolean = underlying.writableBytes == width

  def writeBoolean(b: Boolean): BufferWriter = if(b) writeByte(0) else writeByte(1)

  def writeByte(n: Int): BufferWriter = {
    underlying.writeByte(n)
    this
  }

  def writeShort(n: Int): BufferWriter = {
    underlying.writeShort(n)
    this
  }

  def writeInt24(n: Int): BufferWriter = {
    underlying.writeMedium(n)
    this
  }

  def writeInt(n: Int): BufferWriter = {
    underlying.writeInt(n)
    this
  }

  def writeLong(n: Long): BufferWriter = {
    underlying.writeLong(n)
    this
  }

  def writeFloat(f: Float): BufferWriter = {
    underlying.writeFloat(f)
    this
  }

  def writeDouble(d: Double): BufferWriter = {
    underlying.writeDouble(d)
    this
  }

  def skip(n: Int) = {
    underlying.writerIndex(offset + n)  
    this
  }

  def writeBytes(bytes: Array[Byte]) = {
    underlying.writeBytes(bytes)
    this
  }

  def buffer = underlying.array
  def toChannelBuffer = underlying
}
