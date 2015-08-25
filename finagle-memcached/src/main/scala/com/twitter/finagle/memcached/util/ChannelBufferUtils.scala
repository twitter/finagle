package com.twitter.finagle.memcached.util

import collection.mutable.ArrayBuffer
import com.google.common.base.Strings
import com.twitter.io.Charsets
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers, ChannelBufferIndexFinder}
import scala.language.implicitConversions

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

  private val Byte0 = '0'.toByte

  class RichChannelBuffer(val buffer: ChannelBuffer) extends AnyVal {
    /**
     * Converts `buffer` to a positive integer.
     *
     * We assume an encoding which corresponds with ASCII for the valid range
     * `'0'.toByte` (48) through `'9'.toByte` (57).
     *
     * This conversion can fail if: the buffer is empty, too long,
     * or the buffer contains a byte `b` where `48 <= b <= 57` is false.
     */
    def toInt: Int = {
      val off = buffer.readerIndex()
      val len = buffer.readableBytes()
      if (len == 0)
        throw new NumberFormatException("No readable bytes")
      if (len > 10)
        throw new NumberFormatException("Buffer is larger than max int value")

      var sum = 0
      var i = 0
      while (i < len) {
        val digit = buffer.getByte(off + i) - Byte0
        if (digit < 0 || digit > 9)
          throw new NumberFormatException(s"Not an int: $toString")
        sum *= 10
        sum += digit
        i += 1
      }
      if (sum < 0)
        throw new NumberFormatException(s"Int overflow: $toString")
      sum
    }

    override def toString: String = buffer.toString(Charsets.Utf8)

    def split: Seq[ChannelBuffer] =
      split(FIND_SPACE, 1)

    def split(indexFinder: ChannelBufferIndexFinder, delimiterLength: Int): Seq[ChannelBuffer] = {
      val tokens = new ArrayBuffer[ChannelBuffer](5)
      var scratch = buffer
      while (scratch.capacity > 0) {
        val tokenLength = scratch.bytesBefore(indexFinder)

        if (tokenLength < 0) {
          tokens += scratch.copy
          scratch = ChannelBuffers.EMPTY_BUFFER
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

  implicit def channelBufferToRichChannelBuffer(buffer: ChannelBuffer): RichChannelBuffer =
    new RichChannelBuffer(buffer)

  implicit def stringToChannelBuffer(string: String): ChannelBuffer =
    if(Strings.isNullOrEmpty(string)) null else {
      ChannelBuffers.copiedBuffer(string, Charsets.Utf8)
    }

  implicit def seqOfStringToSeqOfChannelBuffer(strings: Seq[String]): Seq[ChannelBuffer] =
    if (strings == null) null else {
      strings.map { string =>
        if(Strings.isNullOrEmpty(string)) null else {
          ChannelBuffers.copiedBuffer(string, Charsets.Utf8)
        }
      }
    }

  implicit def stringToByteArray(string: String): Array[Byte] =
    string.getBytes

  implicit def stringToChannelBufferIndexFinder(string: String): ChannelBufferIndexFinder =
    new ChannelBufferIndexFinder {
      def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
        var i = 0
        while (i < string.length) {
          if (buffer.getByte(guessedIndex + i) != string.charAt(i).toByte)
            return false
          i += 1
        }
        true
      }
    }
}
