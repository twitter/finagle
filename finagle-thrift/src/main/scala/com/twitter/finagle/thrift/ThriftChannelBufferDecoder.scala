package com.twitter.finagle.thrift

/**
 * ThriftChannel decoder: this simply converts the underlying
 * ChannelBuffers (which have been deframed) into byte arrays.
 */

import org.jboss.netty.channel.{ChannelHandlerContext, Channel}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder

private[thrift] class ThriftChannelBufferDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, ch: Channel, message: Object) = {
    message match {
      case buffer: ChannelBuffer => buffer.array()  // is this kosher?
      case _ => throw new IllegalArgumentException("no byte buffer")
    }
  }
}
