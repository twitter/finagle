package com.twitter.finagle.integration

import java.net.SocketAddress

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import org.jboss.netty.channel.{
  Channel, ChannelFactory, ChannelPipeline,
  ChannelPipelineFactory, Channels, ChannelConfig,
  DefaultChannelConfig}

import com.twitter.util.Future

import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ReferenceCountedChannelFactory}
import com.twitter.finagle.channel.ChannelService

trait IntegrationBase extends Specification with Mockito {
  /*
   * Bootstrap enough to get a basic client connection up & running.
   */
  class MockChannel {
    val codec = mock[Codec[String, String]]
    (codec.prepareService(Matchers.any[Service[String, String]])
     answers { s => Future.value(s.asInstanceOf[Service[String, String]]) })
    (codec.prepareFactory(Matchers.any[ServiceFactory[String, String]])
     answers { f => f.asInstanceOf[ServiceFactory[String, String]] })

    val clientAddress = new SocketAddress {}

    // Pipeline
    val clientPipelineFactory = mock[ChannelPipelineFactory]
    val channelPipeline = mock[ChannelPipeline]
    clientPipelineFactory.getPipeline returns channelPipeline
    codec.pipelineFactory returns clientPipelineFactory

    // Channel
    val channelFactory = mock[ChannelFactory]
    val refcountedChannelFactory = new ReferenceCountedChannelFactory(channelFactory)
    val channel = mock[Channel]
    val connectFuture = spy(Channels.future(channel, true))
    val closeFuture = spy(Channels.future(channel))
    channel.getCloseFuture returns closeFuture
    val channelConfig = new DefaultChannelConfig
    channel.getConfig() returns channelConfig
    channel.connect(clientAddress) returns connectFuture
    channel.getPipeline returns channelPipeline
    channelFactory.newChannel(channelPipeline) returns channel

    val codecFactory = Function.const(codec) _

    val clientBuilder = ClientBuilder()
      .codec(codecFactory)
      .channelFactory(refcountedChannelFactory)
      .hosts(Seq(clientAddress))
      .hostConnectionLimit(1)

    def build() = clientBuilder.build()
  }
}
