package com.twitter.finagle

/**
 * Codecs provide protocol encoding and decoding via netty pipelines
 * as well as a standard filter stack that are applied to services
 * from this codec.
 */

import org.jboss.netty.channel.ChannelPipelineFactory
import com.twitter.util.Future

/**
 * Superclass for all codecs.
 */
trait AbstractCodec[Req, Rep] {
  type IReq = Req
  type IRep = Rep

  /**
   * The pipeline factory that implements the protocol.
   */
  def pipelineFactory: ChannelPipelineFactory

  /**
   * Prepare a newly-created connected Service endpoint. It becomes
   * available once the returned Future is satisfied.
   */
  def prepareService(
    underlying: Service[IReq, IRep]
  ): Future[Service[Req, Rep]] = Future.value(underlying)
}

trait ClientCodec[Req, Rep] extends AbstractCodec[Req, Rep]
trait ServerCodec[Req, Rep] extends AbstractCodec[Req, Rep]

/**
 * A combined codec provides both client and server codecs in one
 * (when available). These are also the legacy codecs, and retains a
 * backwards-compatible interface.
 */
trait Codec[Req, Rep] {
  def clientCodec: ClientCodec[Req, Rep] =
    new ClientCodec[Req, Rep] {
      def pipelineFactory = clientPipelineFactory
      override def prepareService(underlying: Service[Req, Rep]) =
        prepareClientChannel(underlying)
    }

  def serverCodec: ServerCodec[Req, Rep] =
    new ServerCodec[Req, Rep] {
      def pipelineFactory = serverPipelineFactory
      override def prepareService(underlying: Service[Req, Rep]) =
        Future.value(wrapServerChannel(underlying))
    }

  @deprecated("clientPipelineFactory is deprecated, use clientCodec instead")
  val clientPipelineFactory: ChannelPipelineFactory = null
  @deprecated("serverPipelineFactory is deprecated, use serverCodec instead")
  val serverPipelineFactory: ChannelPipelineFactory = null

  @deprecated("prepareClientChannel is deprecated, use clientCodec.prepareService instead")
  def prepareClientChannel(
    underlying: Service[Req, Rep]
  ): Future[Service[Req, Rep]] = Future.value(underlying)

  @deprecated("wrapServerChannel is deprecated, use ServerCodec.prepareService instead")
  def wrapServerChannel(service: Service[Req, Rep]): Service[Req, Rep] = service
}

/**
 * A Protocol describes a complete protocol. Currently this is
 * specified by a Codec and a prepareChannel.
 */
trait Protocol[Req, Rep] {
  def codec: Codec[Req, Rep]
  def prepareChannel(
    underlying: Service[Req, Rep]
  ): Future[Service[Req, Rep]] = Future.value(underlying)
}
