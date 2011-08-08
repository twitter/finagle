package com.twitter.finagle.thrift

import org.jboss.netty.channel.ChannelPipelineFactory
import com.twitter.finagle.{Codec, CodecFactory, ServerCodecConfig}
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory}

/**
 * ThriftServerBufferedCodec implements a buffered thrift transport.
 */
object ThriftServerBufferedCodec {
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftServerBufferedCodecFactory]]
   */
  def apply() = new ThriftServerBufferedCodecFactory
}

class ThriftServerBufferedCodecFactory extends
  CodecFactory[Array[Byte], Array[Byte]]#Server
{
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftServerBufferedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ServerCodecConfig) = {
    new ThriftServerBufferedCodec(new TBinaryProtocol.Factory(), config)
  }
}

class ThriftServerBufferedCodec(protocolFactory: TProtocolFactory, config: ServerCodecConfig)
  extends ThriftServerFramedCodec(config)
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

