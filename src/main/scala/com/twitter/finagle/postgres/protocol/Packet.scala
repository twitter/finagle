package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

object Packet {
  val INT_SIZE = 4
}

case class Packet(code: Option[Char], length: Int, content: ChannelBuffer) {
  
  def encode(): ChannelBuffer = {
    val result = ChannelBuffers.dynamicBuffer()
    code.map { c =>
      result.writeByte(c)
    }
    result.writeInt(length + Packet.INT_SIZE)
    result.writeBytes(content)
    result
  }
}

class PacketBuilder(val code: Option[Char]) {
  private val underlying = ChannelBuffers.dynamicBuffer()

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

  def toPacket = new Packet(code, underlying.writerIndex(), underlying)

}

object PacketBuilder {
  def apply(): PacketBuilder = new PacketBuilder(None)

  def apply(code: Char): PacketBuilder = new PacketBuilder(Some(code))
}