package com.twitter.finagle

import org.jboss.netty.channel.ChannelPipelineFactory

import com.twitter.util.Future

import com.twitter.finagle.channel.ChannelService

/**
 * The codec provides protocol encoding via netty pipelines.
 */
trait Codec[Req, Rep] {
  val clientPipelineFactory: ChannelPipelineFactory
  val serverPipelineFactory: ChannelPipelineFactory
}

/**
 * A Protocol describes a complete protocol. Currently this is
 * specified by a Codec and a PrepareConnection.
 */
trait Protocol[Req, Rep] {
  /**
   * A PrepareConnection function is invoked to prepare the connection
   * for requests. For example, this may be used to implement
   * connection-oriented authentication. The function returns a
   * Service[Req, Rep], and so it is free to wrap the underlying
   * ChannelService[Req, Rep] for protocol specific concerns.
   */
  type PrepareChannel = (ChannelService[Req, Rep]) => Future[_ <: ChannelService[Req, Rep]]

  def codec: Codec[Req, Rep]
  def prepareChannel: PrepareChannel = Future.value(_)
}
