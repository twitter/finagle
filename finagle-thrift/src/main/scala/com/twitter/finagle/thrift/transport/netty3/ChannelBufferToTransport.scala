package com.twitter.finagle.thrift.transport.netty3

import org.apache.thrift.transport.TTransport
import org.jboss.netty.buffer.ChannelBuffer

/**
 * Adapts a single Netty ChannelBuffer to a Thrift TTransport
 *
 * @param  underlying  a netty channelBuffer
 *
 */
private[netty3] class ChannelBufferToTransport(underlying: ChannelBuffer) extends TTransport {
  override def isOpen: Boolean = true
  override def open(): Unit = {}
  override def close(): Unit = {}

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
    val bytesToRead = math.min(length, underlying.readableBytes)
    underlying.readBytes(buffer, offset, bytesToRead)
    bytesToRead
  }

  override def write(buffer: Array[Byte], offset: Int, length: Int): Unit = {
    underlying.writeBytes(buffer, offset, length)
  }
}
