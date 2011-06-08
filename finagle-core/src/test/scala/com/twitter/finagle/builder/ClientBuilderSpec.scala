package com.twitter.finagle.builder

import java.net.SocketAddress

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import org.jboss.netty.channel.{
  Channel, ChannelFactory, ChannelPipeline,
  ChannelPipelineFactory, Channels, ChannelConfig,
  DefaultChannelConfig}

import com.twitter.util.{Promise, Return, Future}

import com.twitter.finagle._
import com.twitter.finagle.channel.ChannelService

object ClientBuilderSpec extends Specification with Mockito {
  "ClientBuilder" should {
    val _codec = mock[Codec[Int, Float]]
    (_codec.prepareService(Matchers.any[Service[Int, Float]])
     answers { s => Future.value(s.asInstanceOf[Service[Int, Float]]) })

    val clientAddress = new SocketAddress {}

    // Pipeline
    val clientPipelineFactory = mock[ChannelPipelineFactory]
    val channelPipeline = mock[ChannelPipeline]
    clientPipelineFactory.getPipeline returns channelPipeline
    _codec.pipelineFactory returns clientPipelineFactory

    // Channel
    val channelFactory = mock[ChannelFactory]
    val refcountedChannelFactory = new ReferenceCountedChannelFactory(channelFactory)
    val channel = mock[Channel]
    val connectFuture = Channels.future(channel)
    val channelConfig = new DefaultChannelConfig
    channel.getConfig() returns channelConfig
    channel.connect(clientAddress) returns connectFuture
    channel.getPipeline returns channelPipeline
    channelFactory.newChannel(channelPipeline) returns channel

    val codecFactory = Function.const(_codec) _

    "invoke prepareChannel on connection establishment" in {
      val prepareChannelPromise = new Promise[Service[Int, Float]]

      (_codec.prepareService(Matchers.any[Service[Int, Float]])
       returns prepareChannelPromise)

      // Client
      val client = ClientBuilder()
        .codec(codecFactory)
        .channelFactory(refcountedChannelFactory)
        .hosts(Seq(clientAddress))
        .hostConnectionLimit(1)
        .build()

      val requestFuture = client(123)

      there was no(_codec).prepareService(any)
      there was one(channelFactory).newChannel(channelPipeline)
      connectFuture.setSuccess()
      there was one(_codec).prepareService(any)

      requestFuture.isDefined must beFalse
      val wrappedChannelService = mock[ChannelService[Int, Float]]
      wrappedChannelService(123) returns Future.value(321.0f)
      prepareChannelPromise() = Return(wrappedChannelService)

      requestFuture.isDefined must beTrue
      requestFuture() must be_==(321.0f)
    }

    "releaseExternalResources once all clients are released" in {
      val client1 = ClientBuilder()
        .channelFactory(refcountedChannelFactory)
        .codec(codecFactory)
        .hosts(Seq(clientAddress))
        .hostConnectionLimit(1)
        .build()

      val client2 = ClientBuilder()
        .channelFactory(refcountedChannelFactory)
        .codec(codecFactory)
        .hosts(Seq(clientAddress))
        .hostConnectionLimit(1)
        .build()

      client1.release()
      there was no(channelFactory).releaseExternalResources()
      client2.release()
      there was one(channelFactory).releaseExternalResources()
    }
  }
}

