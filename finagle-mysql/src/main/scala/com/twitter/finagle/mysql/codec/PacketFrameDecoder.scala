package com.twitter.finagle.exp.mysql.codec

import com.twitter.finagle.exp.mysql.protocol.{Packet, BufferReader}
import com.twitter.finagle.exp.mysql.util.BufferUtil
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.FrameDecoder

/**
 * Decodes logical MySQL packets that could be fragmented across
 * frames. MySQL packets are a length encoded set of bytes written
 * in little endian byte order.
 */
class PacketFrameDecoder extends FrameDecoder {
  private[this] val logger = Logger.getLogger("finagle-mysql")
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Packet = {
    if (buffer.readableBytes < Packet.HeaderSize)
      return null

    buffer.markReaderIndex()

    val header = new Array[Byte](Packet.HeaderSize)
    buffer.readBytes(header)
    val br = BufferReader(header)

    val length = br.readInt24()
    val seq  = br.readUnsignedByte()

    if (buffer.readableBytes < length) {
      buffer.resetReaderIndex()
      return null
    }

    val body = new Array[Byte](length)
    buffer.readBytes(body)

    // logger.finest("RECEIVED: MySQL packet (length=%d, seq=%d)\n%s".format(length, seq, BufferUtil.hex(body)))

    Packet(length, seq, body)
  }
}