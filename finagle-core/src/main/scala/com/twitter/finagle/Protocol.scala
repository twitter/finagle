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
 * specified by a Codec and a PrepareChannel.
 */
trait Protocol[Req, Rep] {
  def codec: Codec[Req, Rep]
  def prepareChannel(underlying: ChannelService[Req, Rep]) =
    Future.value(underlying)
}
