package com.twitter.finagle.thrift

import org.jboss.netty.channel.ChannelPipelineFactory
import com.twitter.finagle.{CodecFactory, ClientCodecConfig}
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory}

/**
 * ThriftClientBufferedCodec implements a buffered thrift transport
 * that supports upgrading in order to provide TraceContexts across
 * requests.
 */
object ThriftClientBufferedCodec {
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientBufferedCodecFactory]]
   */
  def apply() = new ThriftClientBufferedCodecFactory
  
  def apply(protocolFactory: TProtocolFactory) =
    new ThriftClientBufferedCodecFactory(protocolFactory)

}

class ThriftClientBufferedCodecFactory(protocolFactory: TProtocolFactory) extends
  CodecFactory[ThriftClientRequest, Array[Byte]]#Client
{
  def this() = this(new TBinaryProtocol.Factory())
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientBufferedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ClientCodecConfig) = {
    new ThriftClientBufferedCodec(protocolFactory, config)
  }
}

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

