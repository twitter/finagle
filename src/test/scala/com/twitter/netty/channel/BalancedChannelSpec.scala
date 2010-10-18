package com.twitter.netty.channel

import org.specs.Specification
import org.jboss.netty.channel.{ChannelSink, ChannelPipeline}

object BalancedChannelSpec extends Specification {
  "BalancedChannel" should {
    val factory = mock[BalancedChannelFactory]
    val pipeline = mock[ChannelPipeline]
    val sink = mock[ChannelSink]
    val balancedChannel = new BalancedChannel(factory, pipeline, sink)

    
  }
}