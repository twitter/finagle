package com.twitter.finagle.thrift

import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.thrift.transport.ThriftClientPreparer
import com.twitter.finagle.thrift.transport.netty3.ThriftClientFramedPipelineFactory
import com.twitter.finagle.transport.Transport
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.channel._

/**
 * ThriftClientFramedCodec implements a framed thrift transport that
 * supports upgrading in order to provide TraceContexts across
 * requests.
 */
object ThriftClientFramedCodec {
  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientFramedCodecFactory]].
   * Passing a ClientId will propagate that information to the server iff the server is a finagle
   * server.
   */
  def apply(clientId: Option[ClientId] = None): ThriftClientFramedCodecFactory =
    new ThriftClientFramedCodecFactory(clientId)

  def get(): ThriftClientFramedCodecFactory = apply()
}

class ThriftClientFramedCodec(
    protocolFactory: TProtocolFactory,
    config: ClientCodecConfig,
    clientId: Option[ClientId] = None,
    useCallerSeqIds: Boolean = false
) extends Codec[ThriftClientRequest, Array[Byte]] {

  override def newClientDispatcher(
    transport: Transport[Any, Any],
    params: Params
  ): Service[ThriftClientRequest, Array[Byte]] = {
    new ThriftSerialClientDispatcher(
      Transport.cast[ThriftClientRequest, Array[Byte]](transport),
      params[param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
    )
  }

  private[this] val preparer = ThriftClientPreparer(
    protocolFactory, config.serviceName,
    clientId, useCallerSeqIds)

  def pipelineFactory: ChannelPipelineFactory =
    ThriftClientFramedPipelineFactory

  override def prepareConnFactory(
    underlying: ServiceFactory[ThriftClientRequest, Array[Byte]],
    params: Stack.Params
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = preparer.prepare(underlying, params)

  override val protocolLibraryName: String = "thrift"
}
