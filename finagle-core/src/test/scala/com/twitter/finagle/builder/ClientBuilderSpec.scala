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
    (_codec.prepareClientChannel(Matchers.any[Service[Int, Float]])
     answers { s => Future.value(s.asInstanceOf[Service[Int, Float]]) })

    val clientAddress = new SocketAddress {}

    // Pipeline
    val clientPipelineFactory = mock[ChannelPipelineFactory]
    val channelPipeline = mock[ChannelPipeline]
    clientPipelineFactory.getPipeline returns channelPipeline
    _codec.clientPipelineFactory returns clientPipelineFactory
    val clientCodec = mock[ClientCodec[Int, Float]]
    _codec.clientCodec returns clientCodec
    clientCodec.pipelineFactory returns clientPipelineFactory
    (clientCodec.prepareService(Matchers.any[Service[Int, Float]])
     answers { s => Future.value(s.asInstanceOf[Service[Int, Float]]) })

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

    "invoke prepareChannel on connection establishment" in {
      val prepareChannelPromise = new Promise[Service[Int, Float]]
      var theUnderlyingService: Service[Int, Float] = null

      val protocol = new Protocol[Int, Float] {
        def codec = _codec
        override def prepareChannel(underlying: Service[Int, Float]) = {
          theUnderlyingService = underlying
          prepareChannelPromise
        }
      }

      // Client
      val client = ClientBuilder()
        .channelFactory(refcountedChannelFactory)
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

    "releaseExternalResources once all clients are released" in {
      val client1 = ClientBuilder()
        .channelFactory(refcountedChannelFactory)
        .codec(_codec)
        .hosts(Seq(clientAddress))
        .build()

      val client2 = ClientBuilder()
        .channelFactory(refcountedChannelFactory)
        .codec(_codec)
        .hosts(Seq(clientAddress))
        .build()

      client1.release()
      there was no(channelFactory).releaseExternalResources()
      client2.release()
      there was one(channelFactory).releaseExternalResources()
    }
  }
}

