package com.twitter.finagle.memcached.util

import org.jboss.netty.buffer.ChannelBuffer
import java.util.regex.Pattern

object ParserUtils {

  /**
   * Prefer using `isDigits(ChannelBuffer)` or `DigitsPattern.matcher(input).matches()`
   */
  val DIGITS = "^\\d+$"

  val DigitsPattern = Pattern.compile(DIGITS)

  /**
   * Returns true if every readable byte in the ChannelBuffer is a digit,
   * false otherwise.
   *
   * See caliper test in finagle-benchmark, about 15x faster and does 0 allocations
   * versus using ChannelBufferUtils.matches(DIGITS)
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

}
