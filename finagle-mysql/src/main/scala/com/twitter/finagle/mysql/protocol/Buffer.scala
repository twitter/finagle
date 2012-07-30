package com.twitter.finagle.mysql.protocol

import java.lang.{Float => JFloat, Double => JDouble}
import java.sql.{Date => SQLDate, Timestamp}
import java.util.Date
import java.util.Calendar
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

class BufferError(msg: String) extends Exception(msg)

/**
 * Provides classes to decode / encode MySQL data types. 
 * Note, the MySQL protocol represents all data in little endian byte order.
 */
class BufferReader(val buffer: Array[Byte], private[this] var offset: Int = 0) {
  require(offset >= 0)
  require(buffer != null)

  val NULL_LENGTH = -1 // null when reading a length coded binary.
  val EMPTY_STRING = ""
  val EMPTY_ARRAY = new Array[Byte](0)

  def readable(width: Int = 1): Boolean = offset + width <= buffer.size

  /**
   * Reads multi-byte numeric values stored in a byte array. 
   * Starts at offset and reads offset+width bytes. The values are
   * assumed to be stored with the low byte first and the result 
   * is returned as a Long.
   */
  def read(width: Int): Long = {
    val n = (offset until offset + width).zipWithIndex.foldLeft(0L) {
      case (result, (b,i)) => result | ((buffer(b) & 0xFFL) << (i*8))
    }
    offset += width
    n
  }

  def readByte() = read(1).toByte
  def readUnsignedByte() = read(1).toShort
  def readShort() = read(2).toShort
  def readUnsignedShort() = read(2).toInt
  def readInt24() = read(3).toInt
  def readInt() = read(4).toInt
  def readLong() = read(8)
  def readFloat() = JFloat.intBitsToFloat(readInt)
  def readDouble() = JDouble.longBitsToDouble(readLong)

  def skip(n: Int) = offset += n
  def takeRest() = take(buffer.size - offset)
  def take(n: Int) = {
    val res = new Array[Byte](n)
    Array.copy(buffer, offset, res, 0, n)
    offset += n
    res
  }

  /**
   * Read MySQL data field - a variable-length number.
   * Depending on the first byte, read a different width from
   * the buffer.
   */
  def readLengthCodedBinary(): Long = {
    val firstByte = readUnsignedByte()
    if (firstByte < 251)
      firstByte
    else
      firstByte match {
        case 251 => NULL_LENGTH
        case 252 => read(2)
        case 253 => read(3)
        case 254 => read(8)
        case _ => 
          throw new BufferError("Corrupt data or client/server are out of sync.")
      }
  }

  def readNullTerminatedString(): String = {
    val result = new StringBuilder()
    while(buffer(offset) != 0)
      result += readByte().toChar

    readByte() // consume null byte
    result.toString
  }

  def readLengthCodedString(): String = {
    val len = readLengthCodedBinary().toInt

    if (len == NULL_LENGTH)
       null
    else if (len == 0)
      EMPTY_STRING
    else
      new String(take(len))
  }

  def readLengthCodedBytes(): Array[Byte] = {
    val len = readLengthCodedBinary().toInt

    if (len == NULL_LENGTH)
      null
    else if (len == 0)
      EMPTY_ARRAY
    else
      take(len)
  }

  def readTimestamp(): Timestamp = {
    val len = readUnsignedByte()
    // if all fields are 0 they are not sent.
    if (len == 0)
      return ZeroTimestamp

    var year, month, day, hour, min, sec, nano = 0

    // Strictly expect year, month, day.
    if (readable(4)) {
      year = readUnsignedShort()
      month = readUnsignedByte()
      day = readUnsignedByte()
    } else throw new BufferError("Invalid Timestamp.")

    // if the time-part is 00:00:00, it isn't sent
    if (readable(3)) {
      hour = readUnsignedByte()
      min = readUnsignedByte()
      sec = readUnsignedByte()
    }

    // if the sub-seconds are 0, they aren't sent.
    if (readable(4))
      nano = readInt()

    val cal = Calendar.getInstance
    cal.set(year, month-1, day, hour, min, sec)

    val ts = new Timestamp(0)
    ts.setTime(cal.getTimeInMillis)
    ts.setNanos(nano)
    ts
  }

  def readDate(): SQLDate = {
    val ts = readTimestamp()
    if (ts.getTime == 0)
      ZeroDate
    else
      new SQLDate(ts.getTime)
  }

  def toChannelBuffer = ChannelBuffers.wrappedBuffer(buffer)
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

  def skip(n: Int) = offset += n
  def fillRest(b: Byte) = fill(buffer.size - offset, b)
  def fill(n: Int, b: Byte) = {
    (offset until offset + n) foreach { j => buffer(j) = b ; offset += 1 }
    this
  }

  def writeBytes(bytes: Array[Byte]) = {
    Array.copy(bytes, 0, buffer, offset, bytes.length)
    offset += bytes.length
  }

  /**
   * Writes a length coded binary according to
   * the MySQL protocol. Note, 251 is reserved to
   * indicate SQL NULL.
   */
  def writeLengthCodedBinary(length: Long) = {
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

  def writeNullTerminatedString(str: String) = {
    writeBytes(str.getBytes)
    writeByte('\0'.toByte)
    this
  }

  def writeLengthCodedString(str: String) = {
    writeLengthCodedBinary(str.length)
    writeBytes(str.getBytes)
    this
  }

  def writeLengthCodedString(strAsBytes: Array[Byte]) = {
    writeLengthCodedBinary(strAsBytes.length)
    writeBytes(strAsBytes)
    this
  }

  def writeTimestamp(t: Timestamp) = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(t.getTime)
    writeUnsignedByte(11)
    writeUnsignedShort(cal.get(Calendar.YEAR))
    writeUnsignedByte(cal.get(Calendar.MONTH))
    writeUnsignedByte(cal.get(Calendar.DATE))
    writeUnsignedByte(cal.get(Calendar.HOUR_OF_DAY))
    writeUnsignedByte(cal.get(Calendar.MINUTE))
    writeUnsignedByte(cal.get(Calendar.SECOND))
    writeInt(t.getNanos)
    this
  }

  def writeDate(d: SQLDate) = writeTimestamp(new Timestamp(d.getTime))
  def writeDate(d: Date) = writeTimestamp(new Timestamp(d.getTime))

  def toChannelBuffer = ChannelBuffers.wrappedBuffer(buffer)
}
