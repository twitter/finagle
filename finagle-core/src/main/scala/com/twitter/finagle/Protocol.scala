package com.twitter.finagle

import org.jboss.netty.channel.ChannelPipelineFactory

import com.twitter.util.Future

import com.twitter.finagle.channel.ChannelService

/**
 * The codec provides protocol encoding via netty pipelines.
 */
// TODO: do we want to split this into client/server codecs?
trait Codec[Req, Rep] {
  val clientPipelineFactory: ChannelPipelineFactory
  val serverPipelineFactory: ChannelPipelineFactory

  def prepareClientChannel(underlying: Service[Req, Rep]): Future[Service[Req, Rep]] =
    Future.value(underlying)

  def wrapServerChannel(service: Service[Req, Rep]): Service[Req, Rep] = service
}

/**
 * A Protocol describes a complete protocol. Currently this is
 * specified by a Codec and a prepareChannel.
 */
trait Protocol[Req, Rep] {
  def codec: Codec[Req, Rep]
  def prepareChannel(underlying: Service[Req, Rep]): Future[Service[Req, Rep]] =
    Future.value(underlying)
}
