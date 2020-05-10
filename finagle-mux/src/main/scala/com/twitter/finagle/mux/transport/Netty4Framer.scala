package com.twitter.finagle.mux.transport

import io.netty.channel.{ChannelHandler, ChannelPipeline}
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}

private[mux] object Netty4Framer {
  val MaxFrameLength = 0x7fffffff
  val LengthFieldOffset = 0
  val LengthFieldLength = 4
  val LengthAdjustment = 0
  val InitialBytesToStrip = 4
  val FrameEncoder: String = "frameEncoder"
  val FrameDecoder: String = "frameDecoder"
}

/**
 * An implementation of a mux framer using netty4 primitives.
 */
private[mux] abstract class Netty4Framer extends (ChannelPipeline => Unit) {
  def bufferManagerName: String
  def bufferManager: ChannelHandler

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast(
      Netty4Framer.FrameDecoder,
      new LengthFieldBasedFrameDecoder(
        Netty4Framer.MaxFrameLength,
        Netty4Framer.LengthFieldOffset,
        Netty4Framer.LengthFieldLength,
        Netty4Framer.LengthAdjustment,
        Netty4Framer.InitialBytesToStrip
      )
    )
    pipeline.addLast(
      Netty4Framer.FrameEncoder,
      new LengthFieldPrepender(Netty4Framer.LengthFieldLength))
    pipeline.addLast(bufferManagerName, bufferManager)
  }
}
