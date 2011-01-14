package com.twitter.finagle.builder

import org.jboss.netty.channel.ChannelPipelineFactory

class IncompleteSpecification(message: String) extends Exception(message)

trait Codec[Req, Rep] {
  val clientPipelineFactory: ChannelPipelineFactory
  val serverPipelineFactory: ChannelPipelineFactory
}