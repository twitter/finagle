package com.twitter.finagle.mysql.codec

import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.mysql.util.BufferUtil
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext, MessageEvent, Channels, ChannelEvent}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

object RequestEncoder extends OneToOneEncoder {
  override def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case req: SimpleRequest if req.cmd == Command.COM_NOOP_GREET => 
        ChannelBuffers.EMPTY_BUFFER
      case req: Request =>
        println("-> Encoding " + req)
        val bytes = req.toByteArray
        BufferUtil.hex(bytes)
        ChannelBuffers.wrappedBuffer(bytes)
      case _ =>
        message
    }
  }
}