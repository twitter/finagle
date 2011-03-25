package com.twitter.finagle.thrift

import org.jboss.netty.channel.ChannelPipelineFactory
import org.apache.thrift.protocol.TProtocolFactory

import com.twitter.finagle.Codec

class ThriftClientBufferedCodec(protocolFactory: TProtocolFactory)
  extends ThriftClientFramedCodec
{
  override def pipelineFactory = {
    val framedPipelineFactory = super.pipelineFactory

    new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = framedPipelineFactory.getPipeline
        pipeline.replace(
          "thriftFrameCodec", "thriftBufferCodec",
          new ThriftBufferCodec(protocolFactory))
        pipeline
      }
    }
  }
}

