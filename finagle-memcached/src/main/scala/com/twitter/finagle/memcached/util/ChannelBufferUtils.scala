package com.twitter.finagle.memcached.util

import collection.mutable.ArrayBuffer
import com.google.common.base.Strings
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers, ChannelBufferIndexFinder}
import org.jboss.netty.util.CharsetUtil

private[finagle] object ChannelBufferUtils {
  private val FIND_SPACE = new ChannelBufferIndexFinder() {
    def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
      val enoughBytesForDelimeter = guessedIndex + 1
      if (buffer.writerIndex < enoughBytesForDelimeter) return false

      val space = buffer.getByte(guessedIndex)
      space == ' '
    }
  }

  // Per memcached protocol, control characters and whitespace cannot be in the key
  // https://github.com/memcached/memcached/blob/master/doc/protocol.txt
  // But both memcached and twemcache are not strictly enforcing this rule currently,
  // we are relaxing the rules here to only eliminita ' '(ws), \r, \n and \0, to
  // make it compatible with our previous validation logic
  val FIND_INVALID_KEY_CHARACTER = new ChannelBufferIndexFinder() {
    def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
      val enoughBytesForDelimeter = guessedIndex + 1
      if (buffer.writerIndex < enoughBytesForDelimeter) return false

      val control = buffer.getByte(guessedIndex)
      control == '\0' || control == '\n' || control == '\r' || control == ' '
    }
  }

  class RichChannelBuffer(buffer: ChannelBuffer) {
    def matches(string: String) = buffer.toString(CharsetUtil.UTF_8).matches(string)
    def toInt = toString.toInt
    def toLong = toString.toLong
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

  def bytesToChannelBuffer(value: Array[Byte]): ChannelBuffer =
    ChannelBuffers.wrappedBuffer(value)

  def channelBufferToBytes(channelBuffer: ChannelBuffer): Array[Byte] = {
    val length = channelBuffer.readableBytes()
    val bytes = new Array[Byte](length)
    channelBuffer.getBytes(channelBuffer.readerIndex(), bytes, 0, length)
    bytes
  }

  def channelBufferToString(channelBuffer: ChannelBuffer): String =
    new String(channelBufferToBytes(channelBuffer))

  implicit def channelBufferToRichChannelBuffer(buffer: ChannelBuffer) =
    new RichChannelBuffer(buffer)

  implicit def stringToChannelBuffer(string: String) =
    if(Strings.isNullOrEmpty(string)) null else {
      ChannelBuffers.copiedBuffer(string, CharsetUtil.UTF_8)
    }

  implicit def seqOfStringToSeqOfChannelBuffer(strings: Seq[String]) =
    if (strings == null) null else {
      strings.map { string =>
        if(Strings.isNullOrEmpty(string)) null else {
          ChannelBuffers.copiedBuffer(string, CharsetUtil.UTF_8)
        }
      }
    }

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
