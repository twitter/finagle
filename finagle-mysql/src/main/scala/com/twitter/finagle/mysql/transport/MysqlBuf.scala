package com.twitter.finagle.mysql.transport

import com.twitter.io.{Buf, BufByteWriter, ByteReader, ProxyByteReader, ProxyByteWriter}
import java.nio.charset.{StandardCharsets, Charset => JCharset}
import scala.collection.mutable.{Buffer => MutableBuffer}

/**
 * MysqlBuf provides convenience methods for reading/writing a logical packet
 * exchanged between a mysql client and server. All data is little endian ordered.
 */
object MysqlBuf {
  val NullLength: Int = -1 // denotes a SQL NULL value when reading a length coded binary.

  def reader(buf: Buf): MysqlBufReader = new MysqlBufReader(buf)

  def reader(bytes: Array[Byte]): MysqlBufReader = reader(Buf.ByteArray.Owned(bytes))

  def writer(bytes: Array[Byte]): MysqlBufWriter = new MysqlBufWriter(BufByteWriter(bytes))

  /**
   * Calculates the size required to store a length
   * according to the MySQL protocol for length coded
   * binary.
   */
  def sizeOfLen(l: Long): Int =
    if (l < 251) 1 else if (l < 65536) 3 else if (l < 16777216) 4 else 9

  /**
   * Peek at the first byte, if it exists
   */
  def peek(buf: Buf): Option[Byte] = {
    if (buf.length > 0) {
      val arr = Array[Byte](0)
      buf.slice(0, 1).write(arr, 0)
      Some(arr(0))
    } else {
      None
    }
  }
}

class MysqlBufReader(buf: Buf) extends ProxyByteReader {
  import MysqlBuf._

  protected val reader: ByteReader = ByteReader(buf)

  /**
   * Take `n` bytes as a byte array
   */
  def take(n: Int): Array[Byte] = {
    Buf.ByteArray.Owned.extract(readBytes(n))
  }

  /**
   * Reads bytes until a null byte is encountered
   */
  def readNullTerminatedBytes(): Array[Byte] = {
    val bytes = MutableBuffer[Byte]()
    var eof = false
    do {
      val b = readByte()
      if (b == 0x00) {
        eof = true
      } else {
        bytes += b
      }
    } while (!eof)
    bytes.toArray
  }

  /**
   * Reads a null-terminated UTF-8 encoded string
   */
  def readNullTerminatedString(): String =
    new String(readNullTerminatedBytes(), StandardCharsets.UTF_8)

  /**
   * Reads a length encoded set of bytes according to the MySQL
   * Client/Server protocol. This is identical to a length coded
   * string except the bytes are returned raw.
   *
   * @return Array[Byte] if length is non-null, or null otherwise.
   */
  def readLengthCodedBytes(): Array[Byte] = {
    readVariableLong() match {
      case NullLength => null
      case 0 => Array.emptyByteArray
      case len if len > Int.MaxValue =>
        throw new IllegalStateException(s"Length-encoded byte size is too large: $len")
      case len => Buf.ByteArray.Owned.extract(readBytes(len.toInt))
    }
  }

  /**
   * Reads a length encoded string according to the MySQL
   * Client/Server protocol. Uses `charset` to decode the string.
   * For more details refer to MySQL documentation.
   *
   * @return a MySQL length coded String starting at
   * offset.
   */
  def readLengthCodedString(charset: JCharset): String = {
    val bytes = readLengthCodedBytes()
    if (bytes != null) {
      new String(bytes, charset)
    } else {
      null
    }
  }

  /**
   * Reads a variable-length numeric value.
   * Depending on the first byte, reads a different width from
   * the buffer. For more info, refer to MySQL Client/Server protocol
   * documentation.
   *
   * @return a numeric value representing the number of
   * bytes expected to follow.
   */
  def readVariableLong(): Long = {
    readUnsignedByte() match {
      case len if len < 251 => len
      case 251 => NullLength
      case 252 => readUnsignedShortLE()
      case 253 => readUnsignedMediumLE()
      case 254 =>
        val longValue = readLongLE()
        if (longValue < 0)
          throw new IllegalStateException(s"Negative length-encoded value: $longValue")
        longValue

      case len => throw new IllegalStateException(s"Invalid length byte: $len")
    }
  }
}

class MysqlBufWriter(underlying: BufByteWriter)
    extends ProxyByteWriter(underlying)
    with BufByteWriter {

  /**
   * Writes `b` to the buffer `num` times
   */
  def fill(num: Int, b: Byte): MysqlBufWriter = {
    var i = 0
    while (i < num) {
      writeByte(b)
      i += 1
    }
    this
  }

  /**
   * Writes a variable length integer according the the MySQL
   * Client/Server protocol. Refer to MySQL documentation for
   * more information.
   */
  def writeVariableLong(length: Long): MysqlBufWriter = {
    if (length < 0) throw new IllegalStateException(s"Negative length-encoded integer: $length")
    if (length < 251) {
      writeByte(length.toInt)
    } else if (length < 65536) {
      writeByte(252)
      writeShortLE(length.toInt)
    } else if (length < 16777216) {
      writeByte(253)
      writeMediumLE(length.toInt)
    } else {
      writeByte(254)
      writeLongLE(length)
    }
    this
  }

  /**
   * Writes a null terminated string onto the buffer encoded as UTF-8
   *
   * @param s String to write.
   */
  def writeNullTerminatedString(s: String): MysqlBufWriter = {
    writeBytes(s.getBytes(StandardCharsets.UTF_8))
    writeByte(0x00)
    this
  }

  /**
   * Writes a length coded string using the MySQL Client/Server
   * protocol in the given charset.
   *
   * @param s String to write to buffer.
   */
  def writeLengthCodedString(s: String, charset: JCharset): MysqlBufWriter = {
    writeLengthCodedBytes(s.getBytes(charset))
  }

  /**
   * Writes a length coded set of bytes according to the MySQL
   * client/server protocol.
   */
  def writeLengthCodedBytes(bytes: Array[Byte]): MysqlBufWriter = {
    writeVariableLong(bytes.length)
    writeBytes(bytes)
    this
  }

  def owned(): Buf = underlying.owned()
}
