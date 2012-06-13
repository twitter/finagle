package com.twitter.finagle.mysql.util

import scala.collection.mutable.StringBuilder

object ByteArrayUtil {

  /**
   * Reads multi-byte numeric values stored in a byte array. 
   * Starts at offset and reads offset+width bytes. The values are
   * assumed to be stored with the low byte first at data(offset) 
   * (i.e. little endian) and the result is returned as a Long.
   */
  def read(data: Array[Byte], offset: Int, width: Int) = {
    require(0 >= width || width <= 8, "Invalid width, will cause overflow.")
    require(offset + width <= data.size, "Invalid combination of dst.size, width, and offset.")
    (offset until offset + width).zipWithIndex.foldLeft(0L) {
      case (result, (b,i)) => result | ((data(b) & 0xFF) << (i*8))
    }
  }

  /**
   * Functionally, the inverse of read.
   * Splits n into 'width' byte chunks and writes the chunks into
   * the dst array starting at offset.
   */
  def write(n: Long, dst: Array[Byte], offset: Int, width: Int) = {
    require(offset + width <= dst.size, "Invalid combination of dst.size, width, and offset.")
    (0 until width) foreach { i =>
      dst(i + offset) = ((n >> (i*8)) & 0xFF).toByte
    }
    dst
  }

  def write(n: Int, dst: Array[Byte], offset: Int, width: Int): Array[Byte] = {
    write(n.toLong, dst, offset, width)
  }

  /**
   * Parses a null ('\0') terminated string from the data byte array.
   */
  def readNullTerminatedString(data: Array[Byte], offset: Int, res: StringBuilder = new StringBuilder()): String = {
    if (data(offset) == 0) 
      res.toString
    else
      readNullTerminatedString(data, offset + 1, res += data(offset).toChar)
  }

  /**
   * Writes a String as a null terminated string into the destination array.
   */
  def writeNullTerminatedString(str: String, dst: Array[Byte], offset: Int): Array[Byte] = {
    require(offset+str.length+1 <= dst.length, "Not enough room in the destination buffer.")
    Array.copy(str.getBytes, 0, dst, offset, str.length)
    dst(offset + str.length) = '\0'.toByte
    dst
  }

  /**
   * Parses a length encoded string from a byte array.
   */
  def readLengthCodedString(data: Array[Byte], offset: Int): String = {
    val size = data(offset)
    val buffer = new Array[Byte](size)
    Array.copy(data, offset + 1, buffer, 0, size)
    new String(buffer)
  }

  /**
   * Writes a String as a length encoded string into the destination array.
   */
   def writeLengthCodedString(str: String, dst: Array[Byte], offset: Int): Array[Byte] = {
    require(offset+str.length+1 <= dst.length, "Not enough room in the destination buffer.")
    dst(offset) = str.length.toByte
    Array.copy(str.getBytes, 0, dst, offset+1, str.length)
    dst
   }

  /**
   * Helper method to do a hex dump of a sequence of bytes.
   */
  def hex(data: Seq[Byte], output: String = ""): Unit = {
    val (begin,end) = data.splitAt(16)
    val hexline = begin.map { "%02X".format(_) } mkString(" ")
    val charline = begin.map { b => if (0x20 <= b && b <= 0x7E) b.toChar else " " } mkString("")
    val line = "%-47s".format(hexline) + "     " + charline
    if (end.isEmpty)
      println(output + line + "\n")
    else
      hex(end, output + line + "\n")
  }
}