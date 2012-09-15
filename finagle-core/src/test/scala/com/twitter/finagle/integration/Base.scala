package com.twitter.finagle.integration

import com.twitter.finagle._
import com.twitter.finagle.builder.{
  ClientBuilder, ReferenceCountedChannelFactory}
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.transport.TransportFactory

import java.net.SocketAddress

import org.jboss.netty.channel.{
  ChannelPipeline, ChannelPipelineFactory, Channels,
  DefaultChannelConfig, Channel, ChannelFactory}
import org.mockito.Matchers
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

trait IntegrationBase extends SpecificationWithJUnit with Mockito {
  /*
   * Bootstrap enough to get a basic client connection up & running.
   */
  class MockChannel {
    val name = "mock_channel"
    val statsReceiver = new InMemoryStatsReceiver

    val codec = mock[Codec[String, String]]
    (codec.prepareConnFactory(any)
     answers { s => s.asInstanceOf[ServiceFactory[String, String]] })
    (codec.prepareServiceFactory(Matchers.any[ServiceFactory[String, String]])
     answers { f => f.asInstanceOf[ServiceFactory[String, String]] })
    codec.mkClientDispatcher returns { mkTrans: TransportFactory =>
      new SerialClientDispatcher(mkTrans())
    }

    val clientAddress = new SocketAddress{}

    // Pipeline
    val clientPipelineFactory = mock[ChannelPipelineFactory]
    val channelPipeline = mock[ChannelPipeline]
    clientPipelineFactory.getPipeline returns channelPipeline
    codec.pipelineFactory returns clientPipelineFactory

/*
    val codec = new Codec[String, String] {
      def pipelineFactory = clientPipelineFactory
    }
*/
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
      .name(name)
      .codec(codecFactory)
      .channelFactory(refcountedChannelFactory)
      .hosts(Seq(clientAddress))
      .reportTo(statsReceiver)
      .hostConnectionLimit(1)

    def build() = clientBuilder.build()
  }
}
