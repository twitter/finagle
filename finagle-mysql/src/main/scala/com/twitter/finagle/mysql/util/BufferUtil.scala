package com.twitter.finagle.exp.mysql.util

object BufferUtil {
  /**
   * Helper method to do a hex dump of a sequence of bytes.
   */
  def hex(data: Seq[Byte], output: StringBuilder = new
StringBuilder()): String = {
    val (begin,end) = data.splitAt(16)
    val hexline = begin.map { "%02X".format(_) } mkString(" ")
    val charline = begin.map { b => if (0x20 <= b && b <= 0x7E)
b.toChar else " " } mkString("")
    val line = "%-47s".format(hexline) + "     " + charline
    val res = output ++= line ++= "\n"
    if (end.isEmpty)
      res.toString
    else
      hex(end, res)
  }
}