package com.twitter.finagle.util

object ByteArrays {
  /**
   * An efficient implementation of adding two Array[Byte] objects togther.
   * About 20x faster than (a ++ b).
   */
  def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
    val res = new Array[Byte](a.length + b.length)
    System.arraycopy(a, 0, res, 0, a.length)
    System.arraycopy(b, 0, res, a.length, b.length)
    res
  }
}
