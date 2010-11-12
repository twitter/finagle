package com.twitter.finagle.channel

import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._

class BrokerServerBootstrap(channelFactory: ChannelFactory)
  extends ServerBootstrap(channelFactory) with BrokerBootstrap

class BrokerClientBootstrap(channelFactory: ChannelFactory)
  extends ClientBootstrap(channelFactory) with BrokerBootstrap

abstract sealed trait BrokerBootstrap <: Bootstrap
{
  override def getPipelineFactory = {
    val outerFactory = super.getPipelineFactory
    new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = outerFactory.getPipeline
        pipeline.addLast("brokerAdapter", new BrokerAdapter)
        pipeline
      }
    }
  }
}
