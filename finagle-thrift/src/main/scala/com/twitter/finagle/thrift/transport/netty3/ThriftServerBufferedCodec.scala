package com.twitter.finagle.thrift.transport.netty3

import com.twitter.finagle.thrift.{Protocols, ThriftServerFramedCodec}
import com.twitter.finagle.{CodecFactory, ServerCodecConfig}
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

/**
 * ThriftServerBufferedCodec implements a buffered thrift transport.
 */
@deprecated("Use the com.twitter.finagle.Thrift object to build a server", "2017-02-10")
object ThriftServerBufferedCodec {
  /**
   * Create a
   * [[com.twitter.finagle.thrift.transport.netty3.ThriftServerBufferedCodecFactory]],
   * using the binary protocol factory.
   */
  def apply(): ThriftServerBufferedCodecFactory = new ThriftServerBufferedCodecFactory

  /**
   * Create a [[com.twitter.finagle.thrift.transport.netty3.ThriftServerBufferedCodecFactory]]
   * using the protocol factory.
   */
  def apply(protocolFactory: TProtocolFactory): ThriftServerBufferedCodecFactory =
    new ThriftServerBufferedCodecFactory(protocolFactory)
}

@deprecated("Use the com.twitter.finagle.Thrift object to build a server", "2017-02-10")
class ThriftServerBufferedCodecFactory(protocolFactory: TProtocolFactory)
  extends CodecFactory[Array[Byte], Array[Byte]]#Server
{
  def this() = this(Protocols.binaryFactory())
  /**
   * Create a [[com.twitter.finagle.thrift.transport.netty3.ThriftServerBufferedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ServerCodecConfig): ThriftServerBufferedCodec = {
    new ThriftServerBufferedCodec(protocolFactory, config)
  }
}

@deprecated("Use the com.twitter.finagle.Thrift object to build a server", "2017-02-10")
class ThriftServerBufferedCodec(protocolFactory: TProtocolFactory, config: ServerCodecConfig)
    extends ThriftServerFramedCodec(config) {
  override def pipelineFactory = ThriftServerBufferedPipelineFactory(protocolFactory)
}
