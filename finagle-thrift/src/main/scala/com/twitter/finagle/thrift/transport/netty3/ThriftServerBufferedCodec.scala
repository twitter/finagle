package com.twitter.finagle.thrift.transport.netty3

import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory}

private[netty3]
case class ThriftServerBufferedPipelineFactory(protocolFactory: TProtocolFactory)
    extends ChannelPipelineFactory {

  def getPipeline(): ChannelPipeline = {
    val pipeline = ThriftServerFramedPipelineFactory.getPipeline()
    pipeline.replace(
      "thriftFrameCodec", "thriftBufferDecoder",
      new ThriftBufferedTransportDecoder(protocolFactory))
    pipeline
  }
}
