package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.ChannelBuffers

class PacketBuilder(header: Option[Char]) {
  private val underlying = ChannelBuffers.dynamicBuffer()
  private val startingPos = header match {
    case Some(char) => {
      writeChar(char)
      1
    }
    case None => 0
  }
  writeInt(0)

  def writeByte(byte: Byte) = {
    underlying.writeByte(byte)
    this
  }

  def writeChar(char: Char) = {
   underlying.writeByte(char)
   this
  }
  def writeInt(int: Int) = {
    underlying.writeInt(int)
    this
  }

  def writeShort(short: Short) = {
    underlying.writeShort(short)
    this
  }

  def writeCString(str: String) = {
    underlying.writeBytes(str.getBytes(Charsets.Utf8))
    underlying.writeByte(0)
    this
  }

  def toChannelBuffer = {
    val index = underlying.writerIndex() - startingPos
    underlying.markWriterIndex()
    underlying.writerIndex(startingPos)
    underlying.writeInt(index)
    underlying.resetWriterIndex()

    underlying
  }

}

object PacketBuilder {
  def apply(): PacketBuilder = new PacketBuilder(None)

  def apply(code: Char): PacketBuilder = new PacketBuilder(Some(code))
}