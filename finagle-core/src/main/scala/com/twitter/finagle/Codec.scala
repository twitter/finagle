package com.twitter.finagle

/**
 * Codecs provide protocol encoding and decoding via netty pipelines
 * as well as a standard filter stack that are applied to services
 * from this codec.
 */

import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.channel.{ChannelPipelineFactory, ChannelPipeline}
import com.twitter.util.Future

/**
 * Superclass for all codecs.
 */
trait Codec[Req, Rep] {
  /**
   * The pipeline factory that implements the protocol.
   */
  def pipelineFactory: ChannelPipelineFactory

  /**
   * Prepare a factory for usage with the codec.
   * Used to allow codec modifications to the service at the top of the network stack.
   */
  def prepareServiceFactory(underlying: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
    underlying

  /**
   * Prepare a connection factory.
   * Used to allow codec modifications to the service at the bottom of the stack
   * (connection level).
   */
  def prepareConnFactory(underlying: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
    underlying

  /**
   * Raw version of `prepareConnFactory` for clients. This allows raw (as-yet untyped) '
   * access to the underlying codec, allowing the implementor to use different message
   * types for communications between the connection factory and the codec.
   *
   * Useful in implementing connection multiplexing.
   */
  def rawPrepareClientConnFactory(underlying: ServiceFactory[Any, Any]): ServiceFactory[Req, Rep] =
    prepareConnFactory(underlying.asInstanceOf[ServiceFactory[Req, Rep]])

  /**
   * Raw version of `prepareConnFactory` for servers. This allows raw (as-yet untyped) '
   * access to the underlying codec, allowing the implementor to use different message
   * types for communications between the connection factory and the codec.
   *
   * Useful in implementing connection multiplexing.
   */
  def rawPrepareServerConnFactory(underlying: ServiceFactory[Req, Rep]): ServiceFactory[Any, Any] =
    prepareConnFactory(underlying).asInstanceOf[ServiceFactory[Any, Any]]
}

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
