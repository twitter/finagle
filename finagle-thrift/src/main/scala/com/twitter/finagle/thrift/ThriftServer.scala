package com.twitter.finagle.thrift

import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.server.{Listener, DefaultServer}
import com.twitter.finagle.{ServiceFactory, ListeningServer, ServerRegistry}
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

/* TODO: Write apologetic comment about how thrift makes everything complicated. */

private object ThriftFramedListenerPipelineFactory extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("thriftFrameCodec", new ThriftFrameCodec)
    pipeline.addLast("byteEncoder", new ThriftServerChannelBufferEncoder)
    pipeline.addLast("byteDecoder", new ThriftChannelBufferDecoder)
    pipeline
  }
}

/**
 * A Listener for the Apache Thrift [[http://people.apache.org/~jfarrell/thrift/0.6.1/javadoc/org/apache/thrift/transport/TFramedTransport.html framed transport]].
 * The transport prefixes a 4-byte big-endian size field to each message
 */
object ThriftFramedListener extends Netty3Listener[Array[Byte], Array[Byte]](
  "thrift", ThriftFramedListenerPipelineFactory)

private case class ThriftBufferedListenerPipelineFactory(protocolFactory: TProtocolFactory)
  extends ChannelPipelineFactory {

  def getPipeline() = {
    val pipeline = ThriftFramedListenerPipelineFactory.getPipeline()
    pipeline.replace(
      "thriftFrameCodec", "thriftBufferDecoder",
      new ThriftBufferDecoder(protocolFactory))
    pipeline
  }
}

/**
 * A Listener for the Apache Thrift buffered transport. The buffered
 * transport reads and writes raw messages directly. In order to
 * delimit messages, then, a protocol factory is needed. This needs
 * to be the same protocol factory that is used higher in the stack.
 *
 * This layering violation is unfortunate, but is inherent in Thrift's
 * construction.
 *
 * @note The buffered transport seems to be deprecated in recent
 * version of Apache Thrift. This is for the better, as it severely
 * compromises modularity in the Thrift stack.
 */
class ThriftBufferedListener(protocolFactory: TProtocolFactory)
  extends Netty3Listener[Array[Byte], Array[Byte]](
    "thrift", ThriftBufferedListenerPipelineFactory(protocolFactory))

object ThriftBufferedListener {
  def apply(protocolFactory: TProtocolFactory): ThriftBufferedListener =
    new ThriftBufferedListener(protocolFactory)
}

/**
 * A Finagle Server for [[http://thrift.apache.org Apache Thrift]]. This
 * server supports the upgraded protocol described in
 * [[com.twitter.finagle.thrift.ThriftClient]].
 */
private[finagle] class ThriftServer(
    listener: Listener[Array[Byte], Array[Byte]], 
    protocolFactory: TProtocolFactory
) extends DefaultServer[Array[Byte], Array[Byte], Array[Byte], Array[Byte]](
    "thrift", listener, new SerialServerDispatcher(_, _),
    prepare = ThriftServerPreparer(protocolFactory, "unknown",
       new InetSocketAddress(0)).prepare(_))
{
  override def serve(
    addr: SocketAddress, 
    factory: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = {
    val boundAddr = addr match {
      case ia: InetSocketAddress => ia
      case _ => new InetSocketAddress(0)
    }
    val serviceName = ServerRegistry.nameOf(addr) getOrElse "unknown"
    val newPrepare = ThriftServerPreparer(protocolFactory, 
      serviceName, boundAddr).prepare(_)
    // This is subtle: copy() downgrades this to a DefaultServer, 
    // and so fails to recurse.
    copy(prepare = newPrepare).serve(addr, factory)
  }
}
