package com.twitter.netty.channel

import org.jboss.netty.channel.{Channel, ChannelPipeline, ChannelFactory}

class BalancedChannelFactory extends ChannelFactory {
  val sink = new BalancedChannelSink

  def newChannel(pipeline: ChannelPipeline): Channel =
    new BalancedChannel(this, pipeline, sink)

  def releaseExternalResources() = ()
}