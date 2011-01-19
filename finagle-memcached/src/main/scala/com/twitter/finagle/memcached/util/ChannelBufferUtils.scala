package com.twitter.finagle.memcached.util

import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

object ChannelBufferUtils {
  implicit def channelBufferToRichChannelBuffer(buffer: ChannelBuffer) = new {
    def matches(string: String) = buffer.toString(CharsetUtil.UTF_8).matches(string)
    def toInt = toString.toInt
    override def toString = buffer.toString(CharsetUtil.UTF_8)
    def size = buffer.writerIndex() - buffer.readerIndex()
  }

  implicit def stringToChannelBuffer(string: String) =
    ChannelBuffers.wrappedBuffer(string.getBytes)

  implicit def seqOfStringToSeqOfChannelBuffer(strings: Seq[String]) =
    strings.map { string => ChannelBuffers.wrappedBuffer(string.getBytes) }

  implicit def stringToByteArray(string: String) =
    string.getBytes
}