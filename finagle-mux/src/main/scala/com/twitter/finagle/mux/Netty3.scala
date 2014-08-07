package com.twitter.finagle.mux

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{
  LengthFieldBasedFrameDecoder, LengthFieldPrepender}

// Note: this lives in a file that doesn't match the class name in order
// to decouple Netty from finagle and isolate everything related to Netty3 into a single file.
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
