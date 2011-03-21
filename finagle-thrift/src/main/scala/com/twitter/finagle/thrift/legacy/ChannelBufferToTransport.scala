package com.twitter.finagle.thrift

import org.apache.thrift.transport.TTransport
import org.jboss.netty.buffer.ChannelBuffer

/**
 * Adapts a single Netty ChannelBuffer to a Thrift TTransport
 *
 * @param  underlying  a netty channelBuffer
 *
 */
private[thrift] class ChannelBufferToTransport(underlying: ChannelBuffer) extends TTransport {
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

/**
 * Adapts input and output Netty ChannelBuffers to a Thrift TTransport
 *
 * @param  input   a netty channelBuffer to be read from
 * @param  output  a netty channelBuffer to write to
 *
 */
private[thrift] class DuplexChannelBufferTransport(input: ChannelBuffer, output: ChannelBuffer) extends TTransport {
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
