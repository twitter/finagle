package com.twitter.finagle.thrift

import com.twitter.finagle.client.{Bridge, DefaultClient, DefaultPool}
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Group, NamedGroup}
import com.twitter.util.Future
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

private object ThriftFramedTransportPipelineFactory extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
    pipeline.addLast("byteEncoder",      new ThriftClientChannelBufferEncoder)
    pipeline.addLast("byteDecoder",      new ThriftChannelBufferDecoder)
    pipeline
  }
}

private[finagle] object ThriftFramedTransporter 
  extends Netty3Transporter[ThriftClientRequest, Array[Byte]](
    name = "thrift",
    pipelineFactory = ThriftFramedTransportPipelineFactory
)

private[finagle] case class ThriftBufferedTransportPipelineFactory(
    protocolFactory: TProtocolFactory
) extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = ThriftFramedTransportPipelineFactory.getPipeline()
    pipeline.replace(
      "thriftFrameCodec", "thriftBufferDecoder",
      new ThriftBufferDecoder(protocolFactory))
    pipeline
  }
}

class ThriftBufferedTransporter(protocolFactory: TProtocolFactory)
  extends Netty3Transporter[ThriftClientRequest, Array[Byte]](
    name = "thrift",
    pipelineFactory = ThriftBufferedTransportPipelineFactory(protocolFactory)
)

object ThriftBufferedTransporter {
  def apply(protocolFactory: TProtocolFactory): ThriftBufferedTransporter =
    new ThriftBufferedTransporter(protocolFactory)
}

/**
 * A Finagle Client for [[http://thrift.apache.org Apache Thrift]].
 *
 * The client uses the standard thrift protocols, with support for
 * both framed and buffered transports. Finagle attempts to upgrade
 * the protocol in order to ship an extra envelope carrying trace IDs
 * and client IDs associated with the request. These are used by
 * Finagle's tracing facilities and may be collected via aggregators
 * like [[http://twitter.github.com/zipkin/ Zipkin]].
 *
 * The negotiation is simple: on connection establishment, an
 * improbably-named method is dispatched on the server. If that
 * method isn't found, we are dealing with a legacy thrift server,
 * and the standard protocol is used. If the remote server is also a
 * finagle server (or any other supporting this extension), we reply
 * to the request, and every subsequent request is dispatched with an
 * envelope carrying trace metadata. The envelope itself is also a 
 * Thrift struct described [[https://github.com/twitter/finagle/blob/master/finagle-thrift/src/main/thrift/tracing.thrift here]].
 */
class ThriftClient(
    transporter: (SocketAddress, StatsReceiver) => 
      Future[Transport[ThriftClientRequest, Array[Byte]]],
    protocolFactory: TProtocolFactory,
    serviceName: String = "unknown",
    clientId: Option[ClientId] = None
) extends DefaultClient[ThriftClientRequest, Array[Byte]](
    name = "thrift",
    endpointer = {
      // The preparer performs the protocol upgrade.
      val preparer = ThriftClientPreparer(protocolFactory, serviceName, clientId)
      val bridged = Bridge[ThriftClientRequest, Array[Byte], ThriftClientRequest, Array[Byte]](
        transporter, new SerialClientDispatcher(_))
      (sa, sr) => preparer.prepare(bridged(sa, sr))
    },
    pool = DefaultPool[ThriftClientRequest, Array[Byte]]()
) {
  protected def superNewClient(group: Group[SocketAddress]) =
    super.newClient(group)

  override def newClient(group: Group[SocketAddress]) = group match {
    case NamedGroup(groupName) =>
      new ThriftClient(transporter, protocolFactory, groupName, clientId).superNewClient(group)
    case g => superNewClient(g)
  }
}
