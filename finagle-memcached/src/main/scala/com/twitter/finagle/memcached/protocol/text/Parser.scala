package com.twitter.finagle.memcached.protocol.text

import org.jboss.netty.buffer.ChannelBufferIndexFinder
import collection.mutable.ArrayBuffer
import org.jboss.netty.buffer.ChannelBuffer

object Parser {
  private[this] val SKIP_SPACE = 1
  val DIGITS = "^\\d+$"

  def tokenize(_buffer: ChannelBuffer) = {
    val tokens = new ArrayBuffer[ChannelBuffer]
    var buffer = _buffer
    while (buffer.capacity > 0) {
      val tokenLength = buffer.bytesBefore(ChannelBufferIndexFinder.LINEAR_WHITESPACE)

      if (tokenLength < 0) {
        tokens += buffer
        buffer = buffer.slice(0, 0)
      } else {
        tokens += buffer.slice(0, tokenLength)
        buffer = buffer.slice(tokenLength + SKIP_SPACE, buffer.capacity - tokenLength - SKIP_SPACE)
      }
    }
    tokens
  }
}

trait Parser[A] {
  def apply(tokens: Seq[ChannelBuffer]): A
  def needsData(tokens: Seq[ChannelBuffer]): Option[Int]
}