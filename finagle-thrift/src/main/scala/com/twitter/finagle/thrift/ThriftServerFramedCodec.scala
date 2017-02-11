package com.twitter.finagle.thrift

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.transport.netty3.ThriftServerFramedPipelineFactory
import com.twitter.finagle.tracing.TraceInitializerFilter
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.channel.ChannelPipelineFactory

@deprecated("Use the com.twitter.finagle.Thrift object to build a server", "2017-02-10")
object ThriftServerFramedCodec {
  def apply(statsReceiver: StatsReceiver = NullStatsReceiver): ThriftServerFramedCodecFactory =
    new ThriftServerFramedCodecFactory(statsReceiver)

  def apply(protocolFactory: TProtocolFactory): ThriftServerFramedCodecFactory =
    new ThriftServerFramedCodecFactory(protocolFactory)

  def get(): ThriftServerFramedCodecFactory = apply()
}

@deprecated("Use the com.twitter.finagle.Thrift object to build a server", "2017-02-10")
class ThriftServerFramedCodecFactory(protocolFactory: TProtocolFactory)
    extends CodecFactory[Array[Byte], Array[Byte]]#Server
{
  def this(statsReceiver: StatsReceiver) =
    this(Protocols.binaryFactory(statsReceiver = statsReceiver))

  def this() = this(NullStatsReceiver)

  def apply(config: ServerCodecConfig): ThriftServerFramedCodec =
    new ThriftServerFramedCodec(config, protocolFactory)
}

@deprecated("Use the com.twitter.finagle.Thrift object to build a server", "2017-02-10")
class ThriftServerFramedCodec(
    config: ServerCodecConfig,
    protocolFactory: TProtocolFactory = Protocols.binaryFactory()
) extends Codec[Array[Byte], Array[Byte]] {
  def pipelineFactory: ChannelPipelineFactory = ThriftServerFramedPipelineFactory

  private[this] val preparer = ThriftServerPreparer(
    protocolFactory, config.serviceName)

  override def prepareConnFactory(
    factory: ServiceFactory[Array[Byte],
    Array[Byte]], params: Stack.Params
  ): ServiceFactory[Array[Byte], Array[Byte]] = preparer.prepare(factory, params)

  override def newTraceInitializer = TraceInitializerFilter.serverModule[Array[Byte], Array[Byte]]

  override val protocolLibraryName: String = "thrift"
}
