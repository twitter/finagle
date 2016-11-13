package com.twitter.finagle.thrift.transport.netty3

import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory, Channels}

private[finagle] object ThriftServerFramedPipelineFactory  extends ChannelPipelineFactory {
  def getPipeline(): ChannelPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
    pipeline.addLast("byteEncoder", new ThriftServerChannelBufferEncoder)
    pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
    pipeline
  }
}
