package com.twitter.finagle.util

object ByteArrays {

  /**
   * An efficient implementation of adding two Array[Byte] objects together.
   * About 20x faster than (a ++ b).
   */
  def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
    val res = new Array[Byte](a.length + b.length)
    System.arraycopy(a, 0, res, 0, a.length)
    System.arraycopy(b, 0, res, a.length, b.length)
    res
  }

  /**
   * Writes the eight bytes of `l` into `bytes` in big-endian order, starting
   * at the position `i`.
   */
  def put64be(bytes: Array[Byte], i: Int, l: Long): Unit = {
    bytes(i) = (l >> 56 & 0xff).toByte
    bytes(i + 1) = (l >> 48 & 0xff).toByte
    bytes(i + 2) = (l >> 40 & 0xff).toByte
    bytes(i + 3) = (l >> 32 & 0xff).toByte
    bytes(i + 4) = (l >> 24 & 0xff).toByte
    bytes(i + 5) = (l >> 16 & 0xff).toByte
    bytes(i + 6) = (l >> 8 & 0xff).toByte
    bytes(i + 7) = (l & 0xff).toByte
  }

  /**
   * Returns the eight bytes of `bytes` in big-endian order, starting at `i`.
   */
  def get64be(bytes: Array[Byte], i: Int): Long = {
    ((bytes(i) & 0xff).toLong << 56) |
      ((bytes(i + 1) & 0xff).toLong << 48) |
      ((bytes(i + 2) & 0xff).toLong << 40) |
      ((bytes(i + 3) & 0xff).toLong << 32) |
      ((bytes(i + 4) & 0xff).toLong << 24) |
      ((bytes(i + 5) & 0xff).toLong << 16) |
      ((bytes(i + 6) & 0xff).toLong << 8) |
      (bytes(i + 7) & 0xff).toLong
  }
}
