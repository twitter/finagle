package com.twitter.finagle.memcached.util

import org.jboss.netty.util.CharsetUtil
import collection.mutable.ArrayBuffer
import org.jboss.netty.buffer.{ChannelBufferIndexFinder, ChannelBuffers, ChannelBuffer}

object ChannelBufferUtils {
  implicit def channelBufferToRichChannelBuffer(buffer: ChannelBuffer) = new {
    def matches(string: String) = buffer.toString(CharsetUtil.UTF_8).matches(string)
    def toInt = toString.toInt
    override def toString = buffer.toString(CharsetUtil.UTF_8)
    def size = buffer.writerIndex() - buffer.readerIndex()

    def split(delimiter: String) = {
      val tokens = new ArrayBuffer[ChannelBuffer]
      val skipDelimiter = delimiter.length
      var scratch = buffer
      while (scratch.capacity > 0) {
        val tokenLength = scratch.bytesBefore(stringToChannelBufferIndexFinder(delimiter))

        if (tokenLength < 0) {
          tokens += scratch.copy
          scratch = scratch.slice(0, 0)
        } else {
          tokens += scratch.slice(0, tokenLength).copy
          scratch = scratch.slice(tokenLength + skipDelimiter, scratch.capacity - tokenLength - skipDelimiter)
        }
      }
      tokens
    }
  }

  implicit def stringToChannelBuffer(string: String) =
    ChannelBuffers.wrappedBuffer(string.getBytes)

  implicit def seqOfStringToSeqOfChannelBuffer(strings: Seq[String]) =
    strings.map { string => ChannelBuffers.wrappedBuffer(string.getBytes) }

  implicit def stringToByteArray(string: String) =
    string.getBytes

  implicit def stringToChannelBufferIndexFinder(string: String): ChannelBufferIndexFinder = new ChannelBufferIndexFinder {
    def find(buffer: ChannelBuffer, guessedIndex: Int) = {
      var i = 0
      string.forall { char =>
        val matches = buffer.getByte(guessedIndex + i) == char
        i += 1
        matches
      }
    }
  }
}