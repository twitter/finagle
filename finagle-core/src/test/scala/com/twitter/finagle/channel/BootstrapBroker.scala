package com.twitter.finagle.channel

import java.net.{InetSocketAddress}
import java.util.concurrent.Executors

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channel, Channels,
                                ChannelFuture, ChannelPipeline,
                                ChannelHandlerContext, ChannelPipelineFactory,
                                ChannelEvent, SimpleChannelDownstreamHandler}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

import org.specs.Specification
import org.specs.mock.Mockito

object BootstrapBrokerSpec extends Specification with Mockito {
  "BootstrapBroker" should {
    val address =  new InetSocketAddress(1024)
    val cf = new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())

    "throw an exception if the bootstrap has no remoteAddress option" in {
      val bs = new BrokerClientBootstrap(cf)
      new BootstrapBroker(bs) must throwAn [IllegalArgumentException]

      bs.setOption("remoteAddress", address)
      new BootstrapBroker(bs) mustNot throwAn [IllegalArgumentException]
    }

    "getting and putting" in {
      var closed = false
      val bs = spy(new BrokerClientBootstrap(cf))
      bs.setOption("remoteAddress", address)
      val broker = new BootstrapBroker(bs) {
        override def close(channel: Channel) {
          channel.close()
        }
      }

      val f = mock[ChannelFuture]
      bs.connect returns f

      broker.getChannel() mustEqual f

      val c = mock[Channel]
      c.getCloseFuture returns mock[ChannelFuture]
      broker.putChannel(c)
      there was one(c).close()
    }
  }
}
