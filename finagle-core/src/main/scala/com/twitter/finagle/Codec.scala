package com.twitter.finagle

/**
 * Codecs provide protocol encoding and decoding via netty pipelines
 * as well as a standard filter stack that are applied to services
 * from this codec.
 */

import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{ClientChannelTransport, Transport}
import com.twitter.util.Closable
import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.channel.{Channel, ChannelPipeline, ChannelPipelineFactory}

/**
 * Superclass for all codecs.
 */
trait Codec[Req, Rep] {
  /**
   * The pipeline factory that implements the protocol.
   */
  def pipelineFactory: ChannelPipelineFactory

  /* Note: all of the below interfaces are scheduled for deprecation in favor of
   * clients/servers
   */

  /**
   * Prepare a factory for usage with the codec. Used to allow codec
   * modifications to the service at the top of the network stack.
   */
  def prepareServiceFactory(underlying: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
    underlying

  /**
   * Prepare a connection factory. Used to allow codec modifications
   * to the service at the bottom of the stack (connection level).
   */
  def prepareConnFactory(underlying: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
    underlying

  /**
   * Note: the below ("raw") interfaces are low level, and require a
   * good understanding of finagle internals to implement correctly.
   * Proceed with care.
   */

  def newClientTransport(ch: Channel, statsReceiver: StatsReceiver): Transport[Req, Rep] =
    new ClientChannelTransport[Req, Rep](ch, statsReceiver)

  def newClientDispatcher(transport: Transport[Req, Rep]): Service[Req, Rep] =
    new SerialClientDispatcher(transport)

  def newServerDispatcher(transport: Transport[Rep, Req], service: Service[Req, Rep]): Closable =
    new SerialServerDispatcher[Req, Rep](transport, service)

  /**
   * Is this Codec OK for failfast? This is a temporary hack to
   * disable failFast for codecs for which it isn't well-behaved.
   */
  def failFastOk = true
}

/**
 * An abstract class version of the above for java compatibility.
 */
abstract class AbstractCodec[Req, Rep] extends Codec[Req, Rep]

object Codec {
  def ofPipelineFactory[Req, Rep](makePipeline: => ChannelPipeline) = new Codec[Req, Rep] {
    def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline = makePipeline
    }
  }

  def ofPipeline[Req, Rep](p: ChannelPipeline) = new Codec[Req, Rep] {
    def pipelineFactory = new ChannelPipelineFactory {
      def getPipeline = p
    }
  }
}

/**
 * Codec factories create codecs given some configuration.
 */

/**
 * Clients
 */
case class ClientCodecConfig(serviceName: String)

/**
 * Servers
 */
case class ServerCodecConfig(serviceName: String, boundAddress: SocketAddress) {
  def boundInetSocketAddress = boundAddress match {
    case ia: InetSocketAddress => ia
    case _ => new InetSocketAddress(0)
  }
}

/**
 * A combined codec factory provides both client and server codec
 * factories in one (when available).
 */
trait CodecFactory[Req, Rep] {
  type Client = ClientCodecConfig => Codec[Req, Rep]
  type Server = ServerCodecConfig => Codec[Req, Rep]

  def client: Client
  def server: Server
}
