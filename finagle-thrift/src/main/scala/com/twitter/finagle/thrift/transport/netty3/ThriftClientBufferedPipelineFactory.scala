package com.twitter.finagle.thrift.transport.netty3

import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory}

private[netty3] case class ThriftClientBufferedPipelineFactory(
    protocolFactory: TProtocolFactory)
  extends ChannelPipelineFactory {
  def getPipeline(): ChannelPipeline = {
    val pipeline = ThriftClientFramedPipelineFactory.getPipeline()
    pipeline.replace(
      "thriftFrameCodec", "thriftBufferDecoder",
      new ThriftBufferedTransportDecoder(protocolFactory))
    pipeline
  }
}
