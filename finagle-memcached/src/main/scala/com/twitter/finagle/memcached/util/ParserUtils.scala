package com.twitter.finagle.memcached.util

import com.twitter.io.Buf
import java.util.regex.Pattern
import org.jboss.netty.buffer.ChannelBuffer

object ParserUtils {

  /**
   * Prefer using `isDigits(ChannelBuffer)` or `DigitsPattern.matcher(input).matches()`
   */
  val DIGITS = "^\\d+$"

  val DigitsPattern = Pattern.compile(DIGITS)

  /**
   * Returns true if every readable byte in the ChannelBuffer is a digit,
   * false otherwise.
   */
  def isDigits(cb: ChannelBuffer): Boolean = {
    val len = cb.readableBytes()
    if (len == 0)
      return false

    val start = cb.readerIndex()
    val end = start + len
    var i = start
    while (i < end) {
      val b = cb.getByte(i)
      if (b < '0' || b > '9')
        return false
      i += 1
    }
    true
  }

  /**
   * @return true iff the Buf is non empty and every byte in the Buf is a digit.
   */
  def isDigits(buf: Buf): Boolean =
    if (buf.isEmpty) false
    else {
      val Buf.ByteArray.Owned(bytes, begin, end) = Buf.ByteArray.coerce(buf)
      var i = begin
      while (i < end) {
        if (bytes(i) < '0' || bytes(i) > '9')
          return false
        i += 1
      }
      true
    }

}
