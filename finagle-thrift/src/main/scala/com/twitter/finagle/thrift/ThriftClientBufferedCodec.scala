package com.twitter.finagle.thrift

import org.jboss.netty.channel.ChannelPipelineFactory
import org.apache.thrift.protocol.TProtocolFactory

import com.twitter.finagle.Codec
import com.twitter.finagle.ClientCodecConfig

class ThriftClientBufferedCodec(protocolFactory: TProtocolFactory, config: ClientCodecConfig)
  extends ThriftClientFramedCodec(protocolFactory, config)
{
  override def pipelineFactory = {
    val framedPipelineFactory = super.pipelineFactory

    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = framedPipelineFactory.getPipeline
        pipeline.replace(
          "thriftFrameCodec", "thriftBufferDecoder",
          new ThriftBufferDecoder(protocolFactory))
        pipeline
      }
    }
  }
}

