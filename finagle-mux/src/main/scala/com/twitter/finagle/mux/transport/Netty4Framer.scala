package com.twitter.finagle.mux.transport

import com.twitter.finagle.netty4.DirectToHeapInboundHandlerName
import com.twitter.finagle.netty4.channel.DirectToHeapInboundHandler
import com.twitter.finagle.netty4.codec.BufCodec
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}

/**
 * An implementation of a mux framer using netty4 primitives.
 */
private[finagle] object Netty4Framer extends (ChannelPipeline => Unit) {
  private val maxFrameLength = 0x7FFFFFFF
  private val lengthFieldOffset = 0
  private val lengthFieldLength = 4
  private val lengthAdjustment = 0
  private val initialBytesToStrip = 4

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
      maxFrameLength,
      lengthFieldOffset,
      lengthFieldLength,
      lengthAdjustment,
      initialBytesToStrip))
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(lengthFieldLength))
    pipeline.addLast(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)
    pipeline.addLast("bufCodec", new BufCodec)
  }
}