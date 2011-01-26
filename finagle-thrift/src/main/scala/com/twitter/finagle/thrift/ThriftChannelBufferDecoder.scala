package com.twitter.finagle.thrift

import org.jboss.netty.channel.{ChannelHandlerContext, Channel}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder

class ThriftChannelBufferDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, ch: Channel, message: Object) = {
    message match {
      case buffer: ChannelBuffer => buffer.array()  // is this kosher?
      case _ => throw new IllegalArgumentException("no byte buffer")
    }
  }
}
