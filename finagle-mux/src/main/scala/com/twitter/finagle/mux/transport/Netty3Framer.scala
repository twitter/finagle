package com.twitter.finagle.mux.transport

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame

/**
 * An implementation of a mux framer using netty3 pipelines.
 */
private[finagle] object Netty3Framer extends ChannelPipelineFactory {

  private val maxFrameLength = 0x7FFFFFFF
  private val lengthFieldOffset = 0
  private val lengthFieldLength = 4
  private val lengthAdjustment = 0
  private val initialBytesToStrip = 4

  /**
   * Frame a netty3 ChannelBuffer in accordance to the mux spec.
   * That is, a mux frame is a 4-byte length encoded set of bytes.
   */
  private class Framer extends SimpleChannelHandler {
    val dec = new frame.LengthFieldBasedFrameDecoder(
      maxFrameLength,
      lengthFieldOffset,
      lengthFieldLength,
      lengthAdjustment,
      initialBytesToStrip)
    val enc = new frame.LengthFieldPrepender(lengthFieldLength)

    override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit =
      dec.handleUpstream(ctx, e)

    override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent): Unit =
      enc.handleDownstream(ctx, e)
  }

  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("framer", new Framer)
    pipeline
  }
}