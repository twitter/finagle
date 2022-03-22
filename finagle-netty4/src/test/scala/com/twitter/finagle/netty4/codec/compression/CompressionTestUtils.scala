package com.twitter.finagle.netty4.codec.compression

import scala.util.Random

object CompressionTestUtils {
  val rand: Random = new Random

  /**
   * 4B
   * @return randomized array
   */
  def verySmallRandBytes(): Array[Byte] = {
    fillArrayWithCompressibleData(new Array[Byte](1 << 2))
  }

  /**
   * 256B
   * @return randomized array
   */
  def smallRandBytes(): Array[Byte] = {
    fillArrayWithCompressibleData(new Array[Byte](1 << 8))
  }

  /**
   * 512kB
   * @return randomized array
   */
  def largeRandBytes(): Array[Byte] = {
    fillArrayWithCompressibleData(new Array[Byte](1 << 19))
  }

  /**
   * 8MB
   * @return randomized array
   */
  def veryLargeRandBytes(): Array[Byte] = {
    fillArrayWithCompressibleData(
      new Array[Byte](1 << 23)
    ) // suggested limit of window from zstd docs
  }

  def fillArrayWithCompressibleData(array: Array[Byte]): Array[Byte] = {
    for (i <- 0 until array.length) {
      array(i) =
        if (i % 4 != 0) 0
        else rand.nextInt.toByte
    }
    array
  }
}
