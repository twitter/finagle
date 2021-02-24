package com.twitter.finagle.mysql.transport

import com.twitter.io.{Buf, ByteReader, ProxyByteReader}
import java.nio.charset.{StandardCharsets, Charset => JCharset}
import scala.collection.mutable.{Buffer => MutableBuffer}

/**
 * A `ByteReader` specialized for dealing with MySQL protocol messages.
 */
class MysqlBufReader(underlying: ByteReader) extends ProxyByteReader(underlying) {
  import MysqlBuf._

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

object MysqlBufReader {

  /**
   * Create a new [[MysqlBufReader]] from an existing `Buf`.
   */
  def apply(buf: Buf): MysqlBufReader =
    new MysqlBufReader(ByteReader(buf))

  /**
   * Create a new [[MysqlBufReader]] from an array of bytes.
   */
  def apply(bytes: Array[Byte]): MysqlBufReader =
    apply(Buf.ByteArray.Owned(bytes))
}
