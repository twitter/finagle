package com.twitter.finagle.thrift.transport.netty3

import com.twitter.finagle.thrift.ThriftClientFramedPipelineFactory
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory}

private[finagle] case class ThriftClientBufferedPipelineFactory(
    protocolFactory: TProtocolFactory)
  extends ChannelPipelineFactory {
  def getPipeline(): ChannelPipeline = {
    val pipeline = ThriftClientFramedPipelineFactory.getPipeline()
    pipeline.replace(
      "thriftFrameCodec", "thriftBufferDecoder",
      new ThriftBufferDecoder(protocolFactory))
    pipeline
  }
}
