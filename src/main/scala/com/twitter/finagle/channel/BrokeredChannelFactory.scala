package com.twitter.finagle.channel

import org.jboss.netty.channel.{Channel, ChannelPipeline, ChannelFactory}

class BrokeredChannelFactory extends ChannelFactory {
  val sink = new BrokeredChannelSink

  def newChannel(pipeline: ChannelPipeline): Channel =
    new BrokeredChannel(this, pipeline, sink)

  def releaseExternalResources() = ()
}