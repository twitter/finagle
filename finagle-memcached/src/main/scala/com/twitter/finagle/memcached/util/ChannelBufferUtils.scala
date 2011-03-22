package com.twitter.finagle.memcached.util

import org.jboss.netty.util.CharsetUtil
import collection.mutable.ArrayBuffer
import org.jboss.netty.buffer.{ChannelBufferIndexFinder, ChannelBuffers, ChannelBuffer}

private[finagle] object ChannelBufferUtils {
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

    def split: Seq[ChannelBuffer] =
      split(FIND_SPACE, 1)

    def split(delimiter: String): Seq[ChannelBuffer] =
      split(stringToChannelBufferIndexFinder(delimiter), delimiter.size)

    def split(indexFinder: ChannelBufferIndexFinder, delimiterLength: Int): Seq[ChannelBuffer] = {
      val tokens = new ArrayBuffer[ChannelBuffer]
      var scratch = buffer
      while (scratch.capacity > 0) {
        val tokenLength = scratch.bytesBefore(indexFinder)

        if (tokenLength < 0) {
          tokens += scratch.copy
          scratch = scratch.slice(0, 0)
        } else {
          tokens += scratch.slice(0, tokenLength).copy
          scratch = scratch.slice(
            tokenLength + delimiterLength,
            scratch.capacity - tokenLength - delimiterLength)
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
