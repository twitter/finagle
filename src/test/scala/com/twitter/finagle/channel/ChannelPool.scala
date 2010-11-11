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
    val c1 = mock[Channel]
    val f1 = Channels.succeededFuture(c1)
    val c2 = mock[Channel]
    val f2 = Channels.succeededFuture(c2)

    bs.connect() returns f1

    "with no Channels, creates a new one and returns it" in {
      val f = cp.reserve()
      f.isSuccess must beTrue
      f.getChannel mustEqual c1
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

  "ConnectionLimitingChannelPool" should {
    val bs = mock[BrokerClientBootstrap]
    val cp = new ConnectionLimitingChannelPool(bs, 10)
    val c1 = mock[Channel]
    c1.isOpen returns true
    val f1 = Channels.succeededFuture(c1)
    val cause = new Exception
    val f2 = Channels.failedFuture(c1, cause)

    bs.connect() returns f1

    "not make more than the specified number of connections" in {
      0 until 10 foreach { i =>
        val f = cp.reserve()
        f.isSuccess must beTrue
        f.isDone must beTrue
      }

      val f = cp.reserve()
      f.isDone must beFalse
      cp.release(c1)
      f.isDone must beTrue
      f.isSuccess must beTrue
      f.getChannel must be_==(c1)
    }

    "discount unhealthy channels" in {
      bs.connect() returns f2

      0 until 10 foreach { i =>
        val f = cp.reserve()
        f.isSuccess must beFalse
        f.isDone must beTrue
        f.getCause must be_==(cause)
      }

      // (Note: no releases were made.)
      
      bs.connect() returns f1

      0 until 10 foreach { i =>
        val f = cp.reserve()
        f.isSuccess must beTrue
        f.isDone must beTrue
      }
    }
  }
}
