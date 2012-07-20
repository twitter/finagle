package com.twitter.finagle.mysql.protocol

import java.lang.{Float => JFloat, Double => JDouble}
import java.sql.{Time, Date, Timestamp}

/**
 * Defines classes to read and write to/from a byte buffer 
 * in little endian byte order.
 */

class BufferReader(val buffer: Array[Byte], private[this] var offset: Int = 0) {
  require(offset >= 0)
  require(buffer != null)

  def readable(width: Int = 1): Boolean = offset + width <= buffer.size

   /**
   * Reads multi-byte numeric values stored in a byte array. 
   * Starts at offset and reads offset+width bytes. The values are
   * assumed to be stored with the low byte first at data(offset) 
   * (i.e. little endian) and the result is returned as a Long.
   */
  def read(width: Int): Long = {
    val n = (offset until offset + width).zipWithIndex.foldLeft(0L) {
      case (result, (b,i)) => result | ((buffer(b) & 0xFFL) << (i*8))
    }
    offset += width
    n
  }

  def readByte = read(1).toByte
  def readUnsignedByte = read(1).toShort
  def readShort = read(2).toShort
  def readUnsignedShort = read(2).toInt
  def readInt24 = read(3).toInt
  def readInt = read(4).toInt
  def readLong = read(8)
  def readFloat = JFloat.intBitsToFloat(readInt)
  def readDouble = JDouble.longBitsToDouble(readLong)

  def skip(n: Int) = offset += n
  def take(n: Int) = {
    val res = buffer.drop(offset).take(n)
    offset += n
    res
  }
  def takeRest = take(buffer.size - offset)

  /**
  * Read MySQL data field - a variable length encoded binary.
  * Depending on the first byte, read a different width from
  * the data array.
  */
  def readLengthCodedBinary: Long = {
    val firstByte = readByte
    if(firstByte < 251)
      firstByte
    else
      firstByte match {
        case 252 => read(2)
        case 253 => read(3)
        case 254 => read(8)
        case _ => -1 //NULL
      }
  }

  def readNullTerminatedString: String = {
    val result = new StringBuilder()
    while(buffer(offset) != 0)
      result += readByte.toChar

    readByte //consume null byte
    result.toString
  }

  def readLengthCodedString: String = {
    val size = readUnsignedByte

    if(size == 0xFB) 
      return "" // NULL string.

    val strBytes = new Array[Byte](size)
    Array.copy(buffer, offset, strBytes, 0, size)
    offset += size
    new String(strBytes)
  }

  /**
   * Read a MySQL binary encoded Timestamp from the buffer.
   */
  def readTimestamp: Timestamp = {
    val len = readUnsignedByte
    if(len == 0)
      return new Timestamp(0)

    var year, month, day, hour, min, sec, nano = 0

    if(readable(4)) {
      year = readUnsignedShort
      month = readUnsignedByte
      day = readUnsignedByte
    }

    if(readable(3)) {
      hour = readUnsignedByte
      min = readUnsignedByte
      sec = readUnsignedByte
    }

    if(readable(4))
      nano = readInt

    val fmt = "%04d-%02d-%02d %02d:%02d:%02d"
    val ts = Timestamp.valueOf(fmt.format(year,month,day,hour,min,sec))
    ts.setNanos(nano)
    ts
  }

  def readDate: Date = new Date(readTimestamp.getTime)

  /**
   * Read a MySQL binary encoded Time field from the buffer.
   */
   def readTime: Time = {
    val len = readUnsignedByte
    if(len == 0)
      return new Time(0)

    val sign = if(readByte == 1) -1 else 1
    val days = readInt
    val hour = readUnsignedByte
    val min = readUnsignedByte
    val sec = readUnsignedByte

    Time.valueOf("%02d:%02d:%02d".format(hour, min, sec))
   }

}

class BufferWriter(val buffer: Array[Byte], private[this] var offset: Int = 0) {
  require(offset >= 0)
  require(buffer != null)

  def writable(width: Int = 1): Boolean = offset + width <= buffer.size

  /**
   * Write multi-byte numeric values onto the the buffer by
   * widening n accross 'width' byte chunks starting at buffer(offset)
   */
  def write(n: Long, width: Int): BufferWriter = {
    (0 until width) foreach { i =>
      buffer(i + offset) = ((n >> (i*8)) & 0xFF).toByte
    }
    offset += width
    this
  }

  def writeBoolean(b: Boolean) = if(b) writeByte(0) else writeByte(1)
  def writeByte(n: Byte) = write(n, 1)
  def writeShort(n: Short) = write(n, 2)
  def writeUnsignedShort(n: Int) = write(n, 2)
  def writeInt24(n: Int) = write(n,  3)
  def writeInt(n: Int) = write(n, 4)
  def writeLong(n: Long) = write(n, 8)
  def writeFloat(f: Float) = writeInt(JFloat.floatToIntBits(f))
  def writeDouble(d: Double) = writeLong(JDouble.doubleToLongBits(d))

  def skip(n: Int) = offset += n
  def fill(n: Int, b: Byte) = {
    (offset until offset + n) foreach { j => buffer(j) = b ; offset += 1 }
    this
  }
  def fillRest(b: Byte) = fill(buffer.size - offset, b)

  def writeNullTerminatedString(str: String) = {
    Array.copy(str.getBytes, 0, buffer, offset, str.length)
    buffer(offset + str.length) = '\0'.toByte
    offset += str.length + 1
    this
  }

  def writeLengthCodedString(str: String) = {
    buffer(offset) = str.length.toByte
    Array.copy(str.getBytes, 0, buffer, offset+1, str.length)
    offset += str.length + 1
    this
   }
}
