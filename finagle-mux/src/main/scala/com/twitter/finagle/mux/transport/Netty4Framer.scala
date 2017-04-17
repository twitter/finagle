package com.twitter.finagle.mux.transport

import com.twitter.finagle.netty4.AnyToHeapInboundHandlerName
import com.twitter.finagle.netty4.channel.AnyToHeapInboundHandler
import com.twitter.finagle.netty4.codec.BufCodec
import io.netty.channel.{ChannelHandler, ChannelPipeline}
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}


/**
 * An implementation of a mux framer using netty4 primitives.
 */
private[mux] abstract class Netty4Framer(needsBufCodec: Boolean) extends (ChannelPipeline => Unit) {

  private val maxFrameLength = 0x7FFFFFFF
  private val lengthFieldOffset = 0
  private val lengthFieldLength = 4
  private val lengthAdjustment = 0
  private val initialBytesToStrip = 4

  def bufferManagerName: String
  def bufferManager: ChannelHandler

  def apply(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
      maxFrameLength,
      lengthFieldOffset,
      lengthFieldLength,
      lengthAdjustment,
      initialBytesToStrip))
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(lengthFieldLength))
    pipeline.addLast(bufferManagerName, bufferManager)
    if (needsBufCodec) pipeline.addLast("bufCodec", BufCodec)
  }
}

/**
 * A mux framer which copies all inbound direct buffers onto the heap.
 */
private[finagle] object CopyingFramer extends Netty4Framer(true) {
  def bufferManager: ChannelHandler = AnyToHeapInboundHandler
  def bufferManagerName: String = AnyToHeapInboundHandlerName
}

/**
 * A mux framer which delegates ref-counting of control messages to the mux
 * implementation. Non-control messages are copied to the heap.
 */
private[finagle] object RefCountingFramer extends Netty4Framer(false) {
  def bufferManager: ChannelHandler = MuxDirectBufferHandler
  def bufferManagerName: String = "refCountingControlPlaneFramer"
}