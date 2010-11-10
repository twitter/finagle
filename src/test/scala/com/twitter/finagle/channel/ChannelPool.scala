package com.twitter.finagle.channel

import java.util.concurrent.Executors

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channel, Channels, ChannelFuture}
import org.specs.Specification
import org.specs.mock.Mockito

object ChannelPoolSpec extends Specification with Mockito {
  "ChannelPool" should {
      val bs = mock[BrokerClientBootstrap]
      val cp = new ChannelPool(bs)
      val f1 = mock[ChannelFuture]
      val c1 = mock[Channel]
      val f2 = mock[ChannelFuture]
      val c2 = mock[Channel]
      f1.getChannel returns c1
      f2.getChannel returns c2
      bs.connect() returns f1

    "with no Channels, creates a new one and returns it" in {
      cp.reserve().getChannel mustEqual c1
      there was one(bs).connect()
    }

    "with no available Channels, creates a new one and returns it" in {
      cp.reserve().getChannel mustEqual c1
      bs.connect returns f2
      cp.reserve().getChannel mustEqual c2
      there were two(bs).connect()
    }

    "unhealthy returned Channels are discarded" in {
      val c3 = mock[Channel]
      c3.isOpen returns false
      cp.release(c3)
      cp.reserve().getChannel mustEqual c1
    }

    "healthy returned Channels are reused" in {
      val c3 = mock[Channel]
      c3.isOpen returns true
      cp.release(c3)
      cp.reserve().getChannel mustEqual c3
    }
  }
}
