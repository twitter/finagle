package com.twitter.finagle.memcached.util

import com.twitter.io.Buf
import scala.collection.mutable.ArrayBuffer

object ParserUtils {

  // Used by byteArrayStringToInt. The maximum length of a non-negative Int in chars
  private[this] val MaxLengthOfIntString = Int.MaxValue.toString.length

  private[this] object isWhitespaceProcessor extends Buf.Processor {
    private[this] val TokenDelimiter: Byte = ' '
    def apply(byte: Byte): Boolean = byte != TokenDelimiter
  }

  private[this] object isDigitProcessor extends Buf.Processor {
    def apply(byte: Byte): Boolean = byte >= '0' && byte <= '9'
  }

  /**
   * @return true if the Buf is non empty and every byte in the Buf is a digit.
   */
  def isDigits(buf: Buf): Boolean =
    if (buf.isEmpty) false
    else -1 == buf.process(isDigitProcessor)

  private[memcached] def splitOnWhitespace(bytes: Buf): Seq[Buf] = {
    val len = bytes.length
    val split = new ArrayBuffer[Buf](6)
    var segmentStart = 0

    while (segmentStart < len) {
      val segmentEnd = bytes.process(segmentStart, len, isWhitespaceProcessor)
      if (segmentEnd == -1) {
        // At the end
        split += bytes.slice(segmentStart, len)
        segmentStart = len // terminate loop
      } else {
        // We don't add an empty Buf instance at the front
        if (segmentEnd != 0) {
          split += bytes.slice(segmentStart, segmentEnd)
        }

        segmentStart = segmentEnd + 1
      }
    }
    split
  }

  /**
   * Converts the `Buf`, representing a non-negative integer in chars,
   * to a base 10 Int.
   * Returns -1 if any of the bytes are not digits, or the length is invalid
   */
  private[memcached] def bufToInt(buf: Buf): Int = {
    val length = buf.length
    if (length > MaxLengthOfIntString) -1
    else {
      var num = 0
      var i = 0
      while (i < length) {
        val b = buf.get(i)
        if (b >= '0' && b <= '9')
          num = (num * 10) + (b - '0')
        else
          return -1
        i += 1
      }
      num
    }
  }
}
