package com.twitter.finagle.mysql.util

object Util {

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

  def read(data: Array[Byte], offset: Int, size: Int) = {
    (offset until offset + size).zipWithIndex.foldLeft(0L) {
      case (n, (b,i)) => n + (data(b) << i)
    }
  }

  def readNullTerminatedString(data: Array[Byte], offset: Int, res: String = ""): String = {
    if (data(offset) == 0)
      res
    else
      readNullTerminatedString(data, offset + 1, res + data(offset).toChar)
  }

  def readLengthCodedString(data: Array[Byte], offset: Int): String = {
    val size = data(offset)
    val buffer = new Array[Byte](size)
    Array.copy(data, offset + 1, buffer, 0, size)
    new String(buffer)
  }

  def write(n: Int, width: Int, dst: Array[Byte], offset: Int) = {
    var d = 0
    (0 until width) foreach { i =>
      dst(i + offset) = ((n >> d) & 0xFF).toByte
      d += 8
    }
    dst
  }

  def concat(a: Array[Byte], b: Array[Byte]) = {
    val buffer = new Array[Byte](a.size + b.size)
    Array.copy(a, 0, buffer, 0, a.size)
    Array.copy(b, 0, buffer, a.size, b.size)
    buffer
  }
}