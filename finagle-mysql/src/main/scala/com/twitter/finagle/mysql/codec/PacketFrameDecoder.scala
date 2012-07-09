package com.twitter.finagle.mysql.codec

import com.twitter.finagle.mysql.protocol.{Packet, BufferReader}
import com.twitter.finagle.mysql.util.BufferUtil
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.FrameDecoder

/**
 * The built in LengthFieldBasedFrameDecoder in Netty
 * doesn't seem to support byte buffers that are encoded in
 * little endian. Thus, a simple custom FrameDecoder is
 * needed to defrag a ChannelBuffer into a logical MySQL packet.
 *
 * MySQL packets are a length encoded set of bytes written
 * in little endian byte order.
 */
class PacketFrameDecoder extends FrameDecoder {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Packet = {
    if(buffer.readableBytes < Packet.headerSize)
    return null

    val header = new Array[Byte](Packet.headerSize)
    buffer.readBytes(header)
    val br = new BufferReader(header)

    val (length, seq) = (br.readInt24, br.readByte)

    if(buffer.readableBytes < length)
    return null

    println("<- Decoding MySQL packet (length=%d, seq=%d)".format(length, seq))
    val body = new Array[Byte](length)
    buffer.readBytes(body)
    BufferUtil.hex(body)
    Packet(length, seq, body)
  }
}