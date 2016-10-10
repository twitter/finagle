package com.twitter.finagle.thrift.transport.netty3

import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory, Channels}

/**
 * A Netty ChannelPipelineFactory for framing and deframing thrift messages
 */
private[finagle]
object ThriftClientFramedPipelineFactory extends ChannelPipelineFactory {
  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
    pipeline.addLast("byteEncoder",      new ThriftClientChannelBufferEncoder)
    pipeline.addLast("byteDecoder",      new ThriftChannelBufferDecoder)
    pipeline
  }
}
