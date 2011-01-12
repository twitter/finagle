package com.twitter.finagle.builder

import java.net.InetSocketAddress

import org.jboss.netty.channel.ChannelPipelineFactory

import com.twitter.util.Duration

class IncompleteSpecification(message: String) extends Exception(message)

trait Codec[Req, Rep] {
  val clientPipelineFactory: ChannelPipelineFactory
  val serverPipelineFactory: ChannelPipelineFactory
}




