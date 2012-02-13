package com.twitter.finagle.builder

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import com.twitter.util.{Promise, Return, Future}

import com.twitter.finagle._
import com.twitter.finagle.channel.ChannelService
import com.twitter.finagle.integration.IntegrationBase
import com.twitter.finagle.tracing.Tracer

object ClientBuilderSpec extends Specification with IntegrationBase with Mockito {
  "ClientBuilder" should {
    "invoke prepareChannel on connection establishment" in {
      val prepareChannelPromise = new Promise[Service[String, String]]

      val m = new MockChannel

      (m.codec.prepareService(Matchers.any[Service[String, String]])
       returns prepareChannelPromise)

      // Client
      val client = m.build()

      val requestFuture = client("123")

      there was no(m.codec).prepareService(any)
      there was one(m.channelFactory).newChannel(m.channelPipeline)
      m.connectFuture.setSuccess()
      there was one(m.codec).prepareService(any)

      requestFuture.isDefined must beFalse
      val wrappedChannelService = mock[ChannelService[String, String]]
      wrappedChannelService("123") returns Future.value("321")
      prepareChannelPromise() = Return(wrappedChannelService)

      requestFuture.isDefined must beTrue
      requestFuture() must be_==("321")
    }

    "releaseExternalResources once all clients are released" in {
      val m = new MockChannel
      val client1 = m.build()
      val client2 = m.build()

      client1.release()
      there was no(m.channelFactory).releaseExternalResources()
      client2.release()
      there was one(m.channelFactory).releaseExternalResources()
    }

    "notify resources when client is released" in {
      val tracer = mock[Tracer]
      var called = false

      val client = new MockChannel().clientBuilder
        .tracerFactory { h =>
          h.onClose { called = true }
          tracer
        }
        .build()

      called must beFalse
      client.release()
      called must beTrue
    }
  }
}

