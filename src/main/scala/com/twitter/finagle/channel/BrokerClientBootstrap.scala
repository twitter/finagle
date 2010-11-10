package com.twitter.finagle.channel

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

class BrokerClientBootstrap(channelFactory: ChannelFactory)
  extends ClientBootstrap
{
  def this() = this(null)

  if (channelFactory ne null)
    setFactory(channelFactory)

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
