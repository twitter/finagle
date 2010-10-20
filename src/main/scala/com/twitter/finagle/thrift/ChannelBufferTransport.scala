package com.twitter.finagle.thrift

import org.apache.commons.lang.NotImplementedException
import org.apache.thrift.transport.TTransport

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

class ChannelBufferTransport(underlying: ChannelBuffer) extends TTransport {
  override def isOpen = true
  override def open() {}
  override def close() {}

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
    val bytesToRead = math.min(length, underlying.readableBytes)
    underlying.readBytes(buffer, offset, bytesToRead)
    bytesToRead
  }

  override def write(buffer: Array[Byte], offset: Int, length: Int) {
    underlying.writeBytes(buffer, offset, length)
  }
}

class DuplexChannelBufferTransport(input: ChannelBuffer, output: ChannelBuffer)
extends TTransport {
  override def isOpen = true
  override def open() {}
  override def close() {}

  override def read(buffer: Array[Byte], offset: Int, length: Int) = {
    val readableBytes = input.readableBytes()
    val bytesToRead = math.min(length, readableBytes)
    input.readBytes(buffer, offset, bytesToRead)
    bytesToRead
  }

  override def write(buffer: Array[Byte], offset: Int, length: Int) {
    output.writeBytes(buffer, offset, length)
  }

}

object ChannelBufferConversions {
  implicit def channelBufferToChannelBufferTransport(buf: ChannelBuffer) =
    new ChannelBufferTransport(buf)
}
