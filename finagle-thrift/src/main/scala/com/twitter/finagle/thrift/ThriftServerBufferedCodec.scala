package com.twitter.finagle.thrift

import org.jboss.netty.channel.ChannelPipelineFactory
import com.twitter.finagle.{CodecFactory, ServerCodecConfig}
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory}

/**
 * ThriftServerBufferedCodec implements a buffered thrift transport.
 */
object ThriftServerBufferedCodec {
  /**
   * Create a
   * [[com.twitter.finagle.thrift.ThriftServerBufferedCodecFactory]],
   * using the binary protocol factory.
   */
  def apply() = new ThriftServerBufferedCodecFactory

  /**
   * Create a [[com.twitter.finagle.thrift.ThriftServerBufferedCodecFactory]]
   * using the protocol factory.
   */
  def apply(protocolFactory: TProtocolFactory) =
    new ThriftServerBufferedCodecFactory(protocolFactory)
}

class ThriftServerBufferedCodecFactory(protocolFactory: TProtocolFactory)
  extends CodecFactory[Array[Byte], Array[Byte]]#Server
{
  def this() = this(new TBinaryProtocol.Factory())
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftServerBufferedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ServerCodecConfig) = {
    new ThriftServerBufferedCodec(protocolFactory, config)
  }
}

class ThriftServerBufferedCodec(
  protocolFactory: TProtocolFactory,
  config: ServerCodecConfig
)
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

