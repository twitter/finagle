package com.twitter.finagle.mysql.codec

import com.twitter.finagle.mysql.protocol.{Packet, BufferReader}
import com.twitter.finagle.mysql.util.BufferUtil

/**
 * Decodes logical MySQL packets that could be fragmented across
 * frames. MySQL packets are a length encoded set of bytes written
 * in little endian byte order.
 */
class PacketFrameDecoder extends FrameDecoder {
  private[this] val log = Logger("finagle-mysql")
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

    log.debug("<- Decoding MySQL packet (length=%d, seq=%d)".format(length, seq))

    val body = new Array[Byte](length)
    buffer.readBytes(body)

    log.debug(BufferUtil.hex(body))

    Packet(length, seq, body)
  }
}