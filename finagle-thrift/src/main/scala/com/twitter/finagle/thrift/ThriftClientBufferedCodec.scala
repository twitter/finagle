package com.twitter.finagle.thrift

import org.jboss.netty.channel.ChannelPipelineFactory
import com.twitter.finagle.{CodecFactory, ClientCodecConfig}
import org.apache.thrift.protocol.TProtocolFactory

private[finagle]
case class ThriftClientBufferedPipelineFactory(protocolFactory: TProtocolFactory)
    extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = ThriftClientFramedPipelineFactory.getPipeline()
    pipeline.replace(
      "thriftFrameCodec", "thriftBufferDecoder",
      new ThriftBufferDecoder(protocolFactory))
    pipeline
  }
}


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

  /**
   * Helpful from Java.
   */
  def get(): ThriftClientBufferedCodecFactory = {
    // This is here to avoid a pitfall from java. Because the ThriftClientBufferedCodec
    // class extends from ThriftClientFramedCodec and scala generates static forwarding
    // methods. Without this, if you call `ThriftClientBufferedCodec.get()` from java,
    // it would end up calling through to `ThriftClientFramedCodec.get()` which would
    // be quite a surprise and the wrong type. Ick.
    apply()
  }

  def apply(protocolFactory: TProtocolFactory) =
    new ThriftClientBufferedCodecFactory(protocolFactory)

}

class ThriftClientBufferedCodecFactory(protocolFactory: TProtocolFactory) extends
  CodecFactory[ThriftClientRequest, Array[Byte]]#Client
{
  def this() = this(Protocols.binaryFactory())
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientBufferedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ClientCodecConfig) = {
    new ThriftClientBufferedCodec(protocolFactory, config)
  }
}

class ThriftClientBufferedCodec(protocolFactory: TProtocolFactory, config: ClientCodecConfig)
    extends ThriftClientFramedCodec(protocolFactory, config) {
  override def pipelineFactory = ThriftClientBufferedPipelineFactory(protocolFactory)
}
