package com.twitter.finagle.mysql.transport

import com.twitter.io.Buf

/**
 * MysqlBuf provides convenience methods for reading/writing a logical packet
 * exchanged between a mysql client and server. All data is little endian ordered.
 */
object MysqlBuf {
  val NullLength: Int = -1 // denotes a SQL NULL value when reading a length coded binary.

  def reader(buf: Buf): MysqlBufReader = MysqlBufReader(buf)

  def reader(bytes: Array[Byte]): MysqlBufReader = MysqlBufReader(bytes)

  def writer(bytes: Array[Byte]): MysqlBufWriter = MysqlBufWriter(bytes)

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
