package com.twitter.finagle.exp.mysql.transport

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
 * A logical packet exchanged between the mysql
 * server and client. A packet consists of a header
 * (size, sequence number) and a body.
 */
trait Packet {
  /**
   * Size of packet body. Encoded in the first 3
   * bytes of the packet.
   */
  def size: Int = body.capacity

  /**
   * The sequence number used by the server
   * for sanity checks. Encoded in the fourth
   * byte of the packet.
   */
  val seq: Short

  val body: Buffer

  /**
   * Encodes this packet using the mysql spec
   * for a packet into a netty3 ChannelBuffer.
   */
  def toChannelBuffer: ChannelBuffer = {
    val header = BufferWriter(new Array[Byte](Packet.HeaderSize))
    header.writeInt24(size)
    header.writeByte(seq)
    ChannelBuffers.wrappedBuffer(header.underlying, body.underlying)
  }
}

object Packet {
  val HeaderSize = 0x04
  val OkByte     = 0x00.toByte
  val ErrorByte  = 0xFF.toByte
  val EofByte    = 0xFE.toByte

  def apply(s: Short, b: Buffer): Packet = new Packet {
    val seq = s
    val body = b
  }
}
