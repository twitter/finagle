package com.twitter.finagle.mysql.protocol

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import com.twitter.finagle.mysql.util.ByteArrayUtil

object Encoder extends OneToOneEncoder {
  override def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case req: CommandRequest if req.cmd == Request.COM_NOOP_GREET => 
        ChannelBuffers.EMPTY_BUFFER
      case req: Request =>
        println("-> Encoding " + req)
        val encodedMsg = req.encoded
        ByteArrayUtil.hex(encodedMsg)
        ChannelBuffers.wrappedBuffer(encodedMsg)
      case _ =>
        message
    }
  }
}