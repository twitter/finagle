package com.twitter.finagle.mux

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{
  LengthFieldBasedFrameDecoder, LengthFieldPrepender}

private[finagle] object PipelineFactory extends ChannelPipelineFactory {
  private class Framer extends SimpleChannelHandler {
    val dec = new LengthFieldBasedFrameDecoder(0x7FFFFFFF, 0, 4, 0, 4)
    val enc = new LengthFieldPrepender(4)

    override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
      dec.handleUpstream(ctx, e)
    override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) =
      enc.handleDownstream(ctx, e)
  }

  def getPipeline() = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("framer", new Framer)
    pipeline
  }
}

