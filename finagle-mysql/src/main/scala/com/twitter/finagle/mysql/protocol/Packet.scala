package com.twitter.finagle.exp.mysql.protocol

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._

/**
 * Represents a logical packet received from MySQL.
 * A MySQL packet consists of a header and body.
 */
case class Packet(header: PacketHeader, body: Array[Byte])

case class PacketHeader(size: Int, seq: Short) {
  lazy val toChannelBuffer = {
    val bw = BufferWriter(new Array[Byte](Packet.HeaderSize))
    bw.writeInt24(size)
    bw.writeByte(seq)
    bw.toChannelBuffer
  }
}

object Packet {
  val HeaderSize = 0x04
  val OkByte     = 0x00.toByte
  val ErrorByte  = 0xFF.toByte
  val EofByte    = 0xFE.toByte

  def apply(size: Int, seq: Short, body: Array[Byte]): Packet =
    Packet(PacketHeader(size, seq), body)

  /**
   * Creates a MySQL Packet as a Netty
   * ChannelBuffer. Useful for sending a Packet
   * down stream through Netty.
   */
  def toChannelBuffer(size: Int, seq: Short, body: ChannelBuffer) = {
    val headerBuffer = PacketHeader(size, seq).toChannelBuffer
    wrappedBuffer(headerBuffer, body)
  }

  def toChannelBuffer(size: Int, seq: Short, body: Array[Byte]): ChannelBuffer =
    toChannelBuffer(size, seq, wrappedBuffer(body))
}