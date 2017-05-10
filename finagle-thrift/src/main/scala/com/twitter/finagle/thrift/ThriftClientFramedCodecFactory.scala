package com.twitter.finagle.thrift

import com.twitter.finagle.{ClientCodecConfig, CodecFactory}
import org.apache.thrift.protocol.TProtocolFactory

@deprecated("Use the com.twitter.finagle.Thrift object to build a client", "2016-12-01")
class ThriftClientFramedCodecFactory(
    clientId: Option[ClientId],
    _useCallerSeqIds: Boolean,
    _protocolFactory: TProtocolFactory)
  extends CodecFactory[ThriftClientRequest, Array[Byte]]#Client {

  def this(clientId: Option[ClientId]) = this(clientId, false, Protocols.binaryFactory())

  def this(clientId: ClientId) = this(Some(clientId))

  def useCallerSeqIds(x: Boolean): ThriftClientFramedCodecFactory =
    new ThriftClientFramedCodecFactory(clientId, x, _protocolFactory)

  /**
   * Use the given protocolFactory in stead of the default `TBinaryProtocol.Factory`
   */
  def protocolFactory(pf: TProtocolFactory): ThriftClientFramedCodecFactory =
    new ThriftClientFramedCodecFactory(clientId, _useCallerSeqIds, pf)

  /**
   * Create a [[com.twitter.finagle.thrift.ThriftClientFramedCodec]]
   * with a default TBinaryProtocol.
   */
  def apply(config: ClientCodecConfig): ThriftClientFramedCodec =
    new ThriftClientFramedCodec(_protocolFactory, config, clientId, _useCallerSeqIds)
}
