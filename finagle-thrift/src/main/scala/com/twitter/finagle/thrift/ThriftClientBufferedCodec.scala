package com.twitter.finagle.thrift

import org.jboss.netty.channel.ChannelPipelineFactory
import org.apache.thrift.protocol.TProtocolFactory

import com.twitter.finagle.Codec

class ThriftClientBufferedCodec(protocolFactory: TProtocolFactory)
  extends Codec[ThriftClientRequest, Array[Byte]]
{
  private[this] val framedCodec = new ThriftClientFramedCodec

  val clientPipelineFactory = {
    val framedPipelineFactory = framedCodec.clientPipelineFactory

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

  val serverPipelineFactory = clientPipelineFactory  
}

