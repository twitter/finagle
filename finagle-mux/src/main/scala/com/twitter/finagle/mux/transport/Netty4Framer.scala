package com.twitter.finagle.mux.transport

import com.twitter.finagle.netty4.codec.BufCodec
import io.netty.channel.{ChannelHandler, ChannelPipeline}
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}

private[transport] object Netty4Framer {
  val MaxFrameLength = 0x7FFFFFFF
  val LengthFieldOffset = 0
  val LengthFieldLength = 4
  val LengthAdjustment = 0
  val InitialBytesToStrip = 4
}

/**
 * An implementation of a mux framer using netty4 primitives.
 */
private[mux] abstract class Netty4Framer extends (ChannelPipeline => Unit) {
  def bufferManagerName: String
  def bufferManager: ChannelHandler

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast(
      "frameDecoder",
      new LengthFieldBasedFrameDecoder(
        Netty4Framer.MaxFrameLength,
        Netty4Framer.LengthFieldOffset,
        Netty4Framer.LengthFieldLength,
        Netty4Framer.LengthAdjustment,
        Netty4Framer.InitialBytesToStrip
      )
    )
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(Netty4Framer.LengthFieldLength))
    pipeline.addLast(bufferManagerName, bufferManager)
  }
}

/**
 * A mux framer which copies all inbound direct buffers onto the heap.
 */
private[finagle] object CopyingFramer extends Netty4Framer {
  def bufferManager: ChannelHandler = BufCodec
  def bufferManagerName: String = "bufCodec"
}
