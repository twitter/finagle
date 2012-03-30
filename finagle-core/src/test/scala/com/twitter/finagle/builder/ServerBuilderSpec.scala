package com.twitter.finagle.builder

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.mockito.Matchers

import java.net.{SocketAddress, InetSocketAddress}

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.{ServerChannel, ChannelFuture}

import com.twitter.util.Future

import com.twitter.finagle._
import com.twitter.finagle.integration.IntegrationBase
import com.twitter.finagle.tracing.Tracer

class ServerBuilderSpec extends SpecificationWithJUnit with IntegrationBase with Mockito {
  "ServerBuilder" should {
    // Codec
    val codec = mock[Codec[String, String]]
    val codecFactory = Function.const(codec) _

    // Channel
    val channel = mock[ServerChannel]
    val channelFuture = mock[ChannelFuture]
    channel.close() returns channelFuture

    // ServerBootstrap
    val bs = mock[ServerBootstrap]
    bs.bind(any) returns channel

    // Service
    val service = mock[Service[String, String]]
    service(Matchers.anyString) returns Future.value("foo")

    val serverBuilder = ServerBuilder()
    .codec(codecFactory)
    .serverBootstrap(bs)
    .name("TestServer")


    "return correct address when localAddress is called" in {
      val address = mock[SocketAddress]
      channel.getLocalAddress() returns address
      val server = serverBuilder.bindTo(address).build(service)

      val result = server.localAddress

      result must be_==(address)
    }

    "build server that notifies when it's closing" in {
      val address = mock[SocketAddress]
      val tracer = mock[Tracer]
      var called = false

      val server = serverBuilder
        .bindTo(address)
        .tracerFactory { h =>
          h.onClose { called = true }
          tracer
        }
        .build(service)

      called must beFalse
      server.close()
      called must beTrue
    }
  }
}
