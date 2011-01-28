package com.twitter.finagle.builder

import java.net.SocketAddress

import org.specs.Specification
import org.specs.mock.Mockito

import org.jboss.netty.channel.{
  Channel, ChannelFactory, ChannelPipeline,
  ChannelPipelineFactory, Channels, ChannelConfig,
  DefaultChannelConfig}

import com.twitter.util.{Promise, Return, Future}

import com.twitter.finagle._
import com.twitter.finagle.channel.ChannelService

object ClientBuilderSpec extends Specification with Mockito {
  "ClientBuilder" should {
    "invoke prepareChannel on connection establishment" in {
      val _codec = mock[Codec[Int, Float]]
      val prepareChannelPromise = new Promise[ChannelService[Int, Float]]
      var theUnderlyingService: ChannelService[Int, Float] = null

      val protocol = new Protocol[Int, Float] {
        def codec = _codec
        override def prepareChannel(underlying: ChannelService[Int, Float]) = {
          theUnderlyingService = underlying
          prepareChannelPromise
        }
      }

      val clientAddress = new SocketAddress {}

      // Pipeline
      val clientPipelineFactory = mock[ChannelPipelineFactory]
      val channelPipeline = mock[ChannelPipeline]
      clientPipelineFactory.getPipeline returns channelPipeline
      _codec.clientPipelineFactory returns clientPipelineFactory

      // Channel
      val channelFactory = mock[ChannelFactory]
      val channel = mock[Channel]
      val connectFuture = Channels.future(channel)
      val channelConfig = new DefaultChannelConfig
      channel.getConfig() returns channelConfig
      channel.connect(clientAddress) returns connectFuture
      channel.getPipeline returns channelPipeline
      channelFactory.newChannel(channelPipeline) returns channel

      // Client
      val client = ClientBuilder()
        .channelFactory(channelFactory)
        .protocol(protocol)
        .hosts(Seq(clientAddress))
        .build()

      val requestFuture = client(123)

      there was one(channelFactory).newChannel(channelPipeline)
      theUnderlyingService must beNull
      connectFuture.setSuccess()
      theUnderlyingService must notBeNull

      requestFuture.isDefined must beFalse
      val wrappedChannelService = mock[ChannelService[Int, Float]]
      wrappedChannelService(123) returns Future.value(321.0f)
      prepareChannelPromise() = Return(wrappedChannelService)

      requestFuture.isDefined must beTrue
      requestFuture() must be_==(321.0f)
    }
  }
}

