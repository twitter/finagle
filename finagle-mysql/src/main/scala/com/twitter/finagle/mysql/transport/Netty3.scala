package com.twitter.finagle.exp.mysql.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.exp.mysql.{Request, Result}
import com.twitter.finagle.netty3.{ChannelSnooper, Netty3Transporter}
import com.twitter.finagle.Stack
import com.twitter.util.NonFatal
import java.util.logging.{Level, Logger}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.FrameDecoder

/**
 * Decodes logical MySQL packets that could be fragmented across
 * frames. MySQL packets are a length encoded set of bytes written
 * in little endian byte order.
 */
class PacketFrameDecoder extends FrameDecoder {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Packet = {
    if (buffer.readableBytes < Packet.HeaderSize)
      return null

    buffer.markReaderIndex()

    val header = new Array[Byte](Packet.HeaderSize)
    buffer.readBytes(header)
    val br = BufferReader(header)

    val length = br.readUnsignedInt24()
    val seq  = br.readUnsignedByte()

    if (buffer.readableBytes < length) {
      buffer.resetReaderIndex()
      return null
    }

    val body = new Array[Byte](length)
    buffer.readBytes(body)

    Packet(seq, Buffer(body))
  }
}

class PacketEncoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) =
    evt.getMessage match {
      case p: Packet =>
        try {
          val cb = p.toChannelBuffer
          Channels.write(ctx, evt.getFuture, cb, evt.getRemoteAddress)
        } catch {
          case NonFatal(e) =>
            evt.getFuture.setFailure(new ChannelException(e.getMessage))
        }

      case unknown =>
        evt.getFuture.setFailure(new ChannelException(
          "Unsupported request type %s".format(unknown.getClass.getName)))
    }
}

/**
 * A Netty3 pipeline that is responsible for framing network
 * traffic in terms of mysql logical packets.
 */
object MysqlClientPipelineFactory extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("packetDecoder", new PacketFrameDecoder)
    pipeline.addLast("packetEncoder", new PacketEncoder)
    pipeline
  }
}

/**
 * Responsible for the transport layer plumbing required to produce
 * a Transporter[Packet, Packet]. The current implementation uses
 * Netty3.
 */
object MysqlTransporter {
  def apply(params: Stack.Params): Transporter[Packet, Packet] =
    Netty3Transporter(MysqlClientPipelineFactory, params)
}
