package com.twitter.finagle.thrift

import org.jboss.netty.channel.{SimpleChannelHandler, ChannelEvent, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}

class ThriftFrameCodec extends SimpleChannelHandler {
  private[this] val decoder = new LengthFieldBasedFrameDecoder(0x7FFFFFFF, 0, 4, 0, 4)
  private[this] val encoder = new LengthFieldPrepender(4)

  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    decoder.handleUpstream(ctx, e)

  override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
    encoder.handleDownstream(ctx, e)
}
