package com.twitter.finagle.thrift

import org.jboss.netty.channel.{
  SimpleChannelHandler, ChannelEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.{
  FrameDecoder, LengthFieldBasedFrameDecoder, LengthFieldPrepender}

class ThriftFrameCodec extends SimpleChannelHandler {
  val decoder = new LengthFieldBasedFrameDecoder(0x7FFFFFFF, 0, 4, 0, 4)
  val encoder = new LengthFieldPrepender(4)

  override def handleUpstream(ctx: ChannelHandlerContext, c: ChannelEvent) =
    decoder.handleUpstream(ctx, c)

  override def handleDownstream(ctx: ChannelHandlerContext, c: ChannelEvent) =
    encoder.handleDownstream(ctx, c)
}
