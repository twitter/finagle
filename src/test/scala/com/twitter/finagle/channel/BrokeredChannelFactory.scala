package com.twitter.finagle.channel

import org.jboss.netty.channel.{Channels, ChannelPipeline,
                                SimpleChannelUpstreamHandler}
import org.specs.Specification
import org.specs.mock.Mockito

object BrokeredChannelFactorySpec extends Specification with Mockito {
  "BrokeredChannelFactory" should {
    val pipeline = Channels.pipeline
    "newChannel returns new return BrokeredChannels" in {
      val p = Channels.pipeline
      p.addLast("hi", new SimpleChannelUpstreamHandler())
      val cf = new BrokeredChannelFactory
      val c = cf.newChannel(p)
      c must haveClass[BrokeredChannel]
      c.getFactory mustEqual cf
      c.getPipeline mustEqual p
    }
  }
}
