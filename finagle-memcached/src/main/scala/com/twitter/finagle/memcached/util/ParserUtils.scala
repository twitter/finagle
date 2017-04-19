package com.twitter.finagle.memcached.util

import com.twitter.io.Buf
import scala.collection.mutable.ArrayBuffer

object ParserUtils {

  // Used by byteArrayStringToInt. The maximum length of a non-negative Int in chars
  private[this] val MaxLengthOfIntString = Int.MaxValue.toString.length

  private[this] val isDigitProcessor = new Buf.Processor {
    def apply(byte: Byte): Boolean = byte >= '0' && byte <= '9'
  }

  /**
   * @return true iff the Buf is non empty and every byte in the Buf is a digit.
   */
  def isDigits(buf: Buf): Boolean =
    if (buf.isEmpty) false
    else -1 == buf.process(isDigitProcessor)

  private[memcached] def split(bytes: Array[Byte], delimiter: Byte): IndexedSeq[Buf] = {
    val split = new ArrayBuffer[Buf](6)
    var segmentStart = 0
    var segmentEnd = 0
    while (segmentEnd < bytes.length) {
      if (bytes(segmentEnd) == delimiter) {
        if (segmentEnd != 0)
          split += Buf.ByteArray.Owned(bytes, segmentStart, segmentEnd)
        segmentStart = segmentEnd + 1
        segmentEnd = segmentStart
      } else {
        segmentEnd += 1
      }
    }
    if (segmentStart != segmentEnd) {
      split += Buf.ByteArray.Owned(bytes, segmentStart, segmentEnd)
    }
    split.toIndexedSeq
  }

  private[memcached] def newByteArrayForBuf2Int() = new Array[Byte](MaxLengthOfIntString)

  /**
   * Converts `length` characters of a Byte Array, representing a non-negative integer in chars,
   * to a base 10 Int.
   * Returns -1 if any of the characters are not digits, or the length is invalid
   */
  private[memcached] def byteArrayStringToInt(bytes: Array[Byte], length: Int): Int = {
    if (length < 0 || length > MaxLengthOfIntString || length > bytes.length) -1
    else {
      var num = 0
      var multiple = 1
      var i = length - 1 // Start at the least significant digit and move left
      while (i >= 0) {
        if (bytes(i) >= '0' && bytes(i) <= '9')
          num += (bytes(i) - '0') * multiple
        else
          return -1
        i -= 1
        multiple *= 10
      }
      num
    }
  }

}
