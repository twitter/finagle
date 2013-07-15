package com.twitter.finagle.thrift

import org.jboss.netty.channel.{ChannelHandlerContext, Channel}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder

/**
 * ThriftChannel decoder: this simply converts the underlying
 * ChannelBuffers (which have been deframed) into byte arrays.
 */
private[thrift] class ThriftChannelBufferDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, ch: Channel, message: Object) = {
    message match {
      case buffer: ChannelBuffer if buffer.hasArray
          && buffer.arrayOffset == 0 && buffer.readerIndex == 0
          && buffer.readableBytes == buffer.array().length =>
        buffer.array()
      case buffer: ChannelBuffer =>
        val arr = new Array[Byte](buffer.readableBytes)
        buffer.getBytes(buffer.readerIndex, arr)
        arr
      case _ => throw new IllegalArgumentException("no byte buffer")
    }
  }
}
