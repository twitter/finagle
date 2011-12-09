package com.twitter.finagle.builder

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers

import java.net.{SocketAddress, InetSocketAddress}

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.ServerChannel

import com.twitter.util.Future

import com.twitter.finagle._
import com.twitter.finagle.integration.IntegrationBase

object ServerBuilderSpec extends Specification with IntegrationBase with Mockito {
  "ServerBuilder" should {
    // Codec
    val codec = mock[Codec[String, String]]
    (codec.prepareService(Matchers.any[Service[String, String]])
    answers { s => Future.value(s.asInstanceOf[Service[String, String]]) })
    val codecFactory = Function.const(codec) _

    // Channel
    val channel = mock[ServerChannel]

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
  }
}