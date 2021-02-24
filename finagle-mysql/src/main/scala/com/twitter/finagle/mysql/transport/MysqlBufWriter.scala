package com.twitter.finagle.mysql.transport

import com.twitter.io.{Buf, BufByteWriter, ProxyByteWriter}
import java.nio.charset.{StandardCharsets, Charset => JCharset}

/**
 * A `ByteWriter` specialized for dealing with MySQL protocol messages.
 */
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

object MysqlBufWriter {

  /**
   * Create a new [[MysqlBufWriter]] from an array of bytes.
   */
  def apply(bytes: Array[Byte]): MysqlBufWriter =
    new MysqlBufWriter(BufByteWriter(bytes))

}
