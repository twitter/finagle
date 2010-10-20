package com.twitter.finagle.thrift

import org.apache.commons.lang.NotImplementedException
import org.apache.thrift.transport.TTransport

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

trait ChannelBufferTransport extends TTransport {
  override def isOpen = true
  override def open() {}
  override def close() {}

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int =
    throw new NotImplementedException
  override def write(buffer: Array[Byte], offset: Int, length: Int): Unit =
    throw new NotImplementedException
}

class WritableChannelBufferTransport(initialSize: Int) extends ChannelBufferTransport {
  def this() = this(4192)

  val writeableBuffer = ChannelBuffers.dynamicBuffer(initialSize)

  override def write(buffer: Array[Byte], offset: Int, length: Int) {
    writeableBuffer.writeBytes(buffer, offset, length)
  }
}

class ReadableChannelBufferTransport(readableBuffer: ChannelBuffer)
extends ChannelBufferTransport
{
  override def read(buffer: Array[Byte], offset: Int, length: Int) = {
    val readableBytes = readableBuffer.readableBytes
    val bytesToRead = if (length > readableBytes) readableBytes else length
    readableBuffer.readBytes(buffer, offset, bytesToRead)
    bytesToRead
  }
}
