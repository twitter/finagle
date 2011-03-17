package com.twitter.finagle.memcached.util

import org.jboss.netty.util.CharsetUtil
import collection.mutable.ArrayBuffer
import org.jboss.netty.buffer.{ChannelBufferIndexFinder, ChannelBuffers, ChannelBuffer}


object ChannelBufferUtils {
  private val FIND_SPACE = new ChannelBufferIndexFinder() {
    def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
      val enoughBytesForDelimeter = guessedIndex + 1
      if (buffer.writerIndex < enoughBytesForDelimeter) return false

      val space = buffer.getByte(guessedIndex)
      space == ' '
    }
  }

  class RichChannelBuffer(buffer: ChannelBuffer) {
    def matches(string: String) = buffer.toString(CharsetUtil.UTF_8).matches(string)
    def toInt = toString.toInt
    override def toString = buffer.toString(CharsetUtil.UTF_8)
    def size = buffer.writerIndex() - buffer.readerIndex()

    def split = {
      val tokens = new ArrayBuffer[ChannelBuffer]
      val skipDelimiter = 1
      var scratch = buffer
      while (scratch.capacity > 0) {
        val tokenLength = scratch.bytesBefore(FIND_SPACE)

        if (tokenLength < 0) {
          tokens += scratch.copy
          scratch = scratch.slice(0, 0)
        } else {
          tokens += scratch.slice(0, tokenLength).copy
          scratch = scratch.slice(
            tokenLength + skipDelimiter,
            scratch.capacity - tokenLength - skipDelimiter)
        }
      }
      tokens
    }
  }

  implicit def channelBufferToRichChannelBuffer(buffer: ChannelBuffer) =
    new RichChannelBuffer(buffer)

  implicit def stringToChannelBuffer(string: String) =
    ChannelBuffers.wrappedBuffer(string.getBytes)

  implicit def seqOfStringToSeqOfChannelBuffer(strings: Seq[String]) =
    strings.map { string => ChannelBuffers.wrappedBuffer(string.getBytes) }

  implicit def stringToByteArray(string: String) =
    string.getBytes

  implicit def stringToChannelBufferIndexFinder(string: String): ChannelBufferIndexFinder =
    new ChannelBufferIndexFinder {
      def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
        val array = string.toArray

        var i: Int = 0
        while (i < string.size) {
          if (buffer.getByte(guessedIndex + i) != array(i).toByte)
            return false
          i += 1
        }

        return true
      }
    }
}
