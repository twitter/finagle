package com.twitter.finagle.exp.mysql.protocol

import com.twitter.finagle.exp.mysql.ClientError
import java.nio.ByteOrder
import java.nio.charset.{Charset => JCharset}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._
import scala.collection.mutable.{Buffer => SBuffer}

/**
 * The BufferReader and BufferWriter interfaces provide methods for
 * reading/writing primitive data types exchanged between the client/server.
 * This includes all primitive numeric types and strings.
 * All Buffer methods are side-effecting. That is, each call to a read* or write*
 * method will increase the current offset.
 *
 * These interfaces are useful for reading from and writing to a MySQL
 * packet. They provide protocol specific methods and assure that
 * buffers are read/written in little endian byte order, which conforms to
 * the MySQL protocol.
 */

object Buffer {
  val NULL_LENGTH = -1 // denotes a SQL NULL value when reading a length coded binary.
  val EMPTY_STRING = new String
  val EMPTY_BYTE_ARRAY = new Array[Byte](0)

  case object CorruptBufferException
    extends Exception("Corrupt data or client/server are out of sync.")

  /**
   * Calculates the size required to store a length
   * according to the MySQL protocol for length coded
   * binary.
   */
  def sizeOfLen(l: Long) =
    if (l < 251) 1 else if (l < 65536) 3 else if (l < 16777216) 4 else 9

  /**
   * Wraps the arrays into a ChannelBuffer with the
   * appropriate MySQL protocol byte order. A wrappedBuffer
   * avoids copying the underlying arrays.
   */
  def toChannelBuffer(bytes: Array[Byte]*) =
    wrappedBuffer(ByteOrder.LITTLE_ENDIAN, bytes: _*)
}

trait BufferReader {
  /**
   * Buffer capacity.
   */
  def capacity: Int = array.size

  /**
   * Current offset in the buffer.
   */
  def offset: Int

  /**
   * Access the underlying array. Note, this
   * is not always a safe operation because the
   * the buffer could contain a composition of
   * arrays, in which case this will throw an
   * exception.
   */
  def array: Array[Byte]

  /**
   * Denotes if the buffer is readable upto the given width
   * based on the current offset.
   */
  def readable(width: Int): Boolean

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
   * Consumes the rest of the buffer and returns
   * it in a new Array[Byte].
   * @return Array[Byte] containing the rest of the buffer.
   */
  def takeRest(): Array[Byte] = take(capacity - offset)

  /**
   * Consumes n bytes in the buffer and
   * returns them in a new Array.
   * @return An Array[Byte] containing bytes from offset to offset+n
   */
  def take(n: Int): Array[Byte]

  /**
   * Reads a MySQL data field. A variable-length numeric value.
   * Depending on the first byte, reads a different width from
   * the buffer. For more info, refer to MySQL Client/Server protocol
   * documentation.
   * @return a numeric value representing the number of
   * bytes expected to follow.
   */
  def readLengthCodedBinary(): Int = {
    val firstByte = readUnsignedByte()
    if (firstByte < 251)
      firstByte
    else
      firstByte match {
        case 251 => Buffer.NULL_LENGTH
        case 252 => readUnsignedShort()
        case 253 => readUnsignedInt24()

        // 254 Indicates a set of bytes with length >= 2^24.
        // The current implementation does not support
        // this.
        case 254 =>
          throw new ClientError("BufferReader: LONG_BLOB is not supported!")
          // readLong()

        case _ =>
          throw Buffer.CorruptBufferException
      }
  }

  /**
   * Reads a null-terminated string where
   * null is denoted by '\0'. Uses Charset.defaultCharset by default
   * to decode strings.
   * @return a null-terminated String starting at offset.
   */
  def readNullTerminatedString(charset: JCharset = Charset.defaultCharset): String = {
    val start = offset
    var length = 0

    while (readByte() != 0x00)
      length += 1

    this.toString(start, length, charset)
  }

  /**
   * Reads a null-terminated array where
   * null is denoted by '\0'.
   * @return a null-terminated String starting at offset.
   */
  def readNullTerminatedBytes(): Array[Byte] = {
    val cur: SBuffer[Byte] = SBuffer()
    do cur += readByte() while (cur.last != 0x00)
    cur.init.toArray
  }

  /**
   * Reads a length encoded string according to the MySQL
   * Client/Server protocol. Uses Charset.defaultCharset by default
   * to decode strings. For more details refer to MySQL
   * documentation.
   * @return a MySQL length coded String starting at
   * offset.
   */
  def readLengthCodedString(charset: JCharset = Charset.defaultCharset): String = {
    val length = readLengthCodedBinary()
    if (length == Buffer.NULL_LENGTH)
       null
    else if (length == 0)
      Buffer.EMPTY_STRING
    else {
      val start = offset
      skip(length)
      this.toString(start, length, charset)
    }
  }

  /**
   * Reads a length encoded set of bytes according to the MySQL
   * Client/Server protocol. This is indentical to a length coded
   * string except the bytes are returned raw.
   * @return an Array[Byte] containing the length coded set of
   * bytes starting at offset.
   */
  def readLengthCodedBytes(): Array[Byte] = {
    val len = readLengthCodedBinary()
    if (len == Buffer.NULL_LENGTH)
      null
    else if (len == 0)
      Buffer.EMPTY_BYTE_ARRAY
    else
      take(len)
  }

  /**
   * Returns the bytes from start to start+length
   * into a string using the given java.nio.charset.Charset.
   */
  def toString(start: Int, length: Int, charset: JCharset) =
    new String(array, start, length, charset)

  /**
   * Returns a Netty ChannelBuffer representing
   * the underlying array. The ChannelBuffer
   * is guaranteed ByteOrder.LITTLE_ENDIAN.
   */
  def toChannelBuffer: ChannelBuffer
}

object BufferReader {
  /**
   * Creates a BufferReader from an Array[Byte].
   * @param bytes Byte array to read from.
   * @param startOffset initial offset.
   */
  def apply(bytes: Array[Byte], startOffset: Int = 0): BufferReader = {
    require(bytes != null)
    require(startOffset >= 0)

    val underlying = Buffer.toChannelBuffer(bytes)
    underlying.readerIndex(startOffset)
    new BufferReaderImpl(underlying)
  }

  /**
   * Creates a BufferReader from a Netty ChannelBuffer.
   * The ChannelBuffer must have ByteOrder.LITTLE_ENDIAN.
   */
  def apply(underlying: ChannelBuffer): BufferReader = {
    require(underlying.order == ByteOrder.LITTLE_ENDIAN)
    new BufferReaderImpl(underlying)
  }

  /**
   * BufferReader implementation backed by a Netty ChannelBuffer.
   */
  private[this] class BufferReaderImpl(underlying: ChannelBuffer) extends BufferReader {
    override def capacity = underlying.capacity
    def offset = underlying.readerIndex
    def array = underlying.array

    def readable(width: Int) = underlying.readableBytes >= width

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

    def take(n: Int) = {
      val res = new Array[Byte](n)
      underlying.readBytes(res)
      res
    }

    /**
     * Forward to ChannelBuffer in case underlying is a composition of
     * arrays.
     */
    override def toString(start: Int, length: Int, charset: JCharset) =
      underlying.toString(start, length, charset)

    def toChannelBuffer = underlying
  }
}

trait BufferWriter {
  /**
   * Buffer capacity.
   */
   def capacity: Int = array.size

  /**
   * Current writer offset.
   */
  def offset: Int

  /**
   * Access the underlying array. Note, this
   * is not always a safe operation because the
   * the buffer could contain a composition of
   * arrays, in which case this will throw an
   * exception.
   */
  def array: Array[Byte]

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
  def fillRest(b: Byte) = fill(capacity - offset, b)

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
    * '\0' denotes null. Uses Charset.defaultCharset by default
    * to decode the given String.
    * @param s String to write.
    */
   def writeNullTerminatedString(
     s: String,
     charset: JCharset = Charset.defaultCharset
   ): BufferWriter = {
    writeBytes(s.getBytes(charset))
    writeByte('\0')
    this
   }

   /**
    * Writes a length coded string using the MySQL Client/Server
    * protocol. Uses Charset.defaultCharset by default to decode
    * the given String.
    * @param s String to write to buffer.
    */
   def writeLengthCodedString(
     s: String,
     charset: JCharset = Charset.defaultCharset
   ): BufferWriter = {
    writeLengthCodedBinary(s.length)
    writeBytes(s.getBytes(charset))
    this
   }

   /**
    * Writes a length coded set of bytes according to the MySQL
    * client/server protocol.
    */
   def writeLengthCodedBytes(bytes: Array[Byte]): BufferWriter = {
    writeLengthCodedBinary(bytes.length)
    writeBytes(bytes)
    this
   }

  /**
   * Returns a Netty ChannelBuffer representing
   * the underlying buffer. The ChannelBuffer
   * is guaranteed ByteOrder.LITTLE_ENDIAN.
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

    // Note, a wrappedBuffer avoids copying the the array.
    val underlying = Buffer.toChannelBuffer(bytes)
    underlying.writerIndex(startOffset)
    new BufferWriterImpl(underlying)
  }

  /**
   * Creates a BufferWriter from a Netty ChannelBuffer.
   */
  def apply(underlying: ChannelBuffer): BufferWriter = {
    require(underlying.order == ByteOrder.LITTLE_ENDIAN)
    new BufferWriterImpl(underlying)
  }

  /**
   * BufferWriter implementation backed by a Netty ChannelBuffer.
   */
  private[this] class BufferWriterImpl(underlying: ChannelBuffer) extends BufferWriter {
    override def capacity = underlying.capacity
    def offset = underlying.writerIndex
    def array = underlying.array
    def writable(width: Int = 1): Boolean = underlying.writableBytes >= width

    def writeBoolean(b: Boolean): BufferWriter = if(b) writeByte(1) else writeByte(0)

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

    def toChannelBuffer = underlying
  }
}
