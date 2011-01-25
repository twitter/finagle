package com.twitter.finagle.thrift

import org.jboss.netty.channel.{
  SimpleChannelHandler, Channel, ChannelEvent, ChannelHandlerContext}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{
  LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}

class ThriftFrameCodec extends SimpleChannelHandler {
  private[this] val decoder = new LengthFieldBasedFrameDecoder(0x7FFFFFFF, 0, 4, 0, 4)
  private[this] val encoder = new LengthFieldPrepender(4)

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    decoder.handleUpstream(ctx, e)

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    encoder.handleDownstream(ctx, e)
}

class ChannelBufferEncoder extends OneToOneEncoder {
  def encode(ctx: ChannelHandlerContext, ch: Channel, message: Object) = {
    message match {
      case array: Array[Byte] => ChannelBuffers.wrappedBuffer(array)
      case _ => throw new IllegalArgumentException("no byte array")
    }
  }
}

class ChannelBufferDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, ch: Channel, message: Object) = {
    message match {
      case buffer: ChannelBuffer => buffer.array()  // is this kosher?
      case _ => throw new IllegalArgumentException("no byte buffer")
    }
  }
}
