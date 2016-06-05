package com.twitter.finagle.builder

import java.net.{InetSocketAddress, InetAddress}

import com.twitter.finagle._
import com.twitter.finagle.integration.{StringCodec, IntegrationBase}
import com.twitter.util._
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
import org.jboss.netty.channel.ChannelPipelineFactory
import org.junit.runner.RunWith
import org.mockito.Mockito.{verify, when}
import org.mockito.Matchers
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

@RunWith(classOf[JUnitRunner])
class ServerBuilderTest extends FunSuite
  with Eventually
  with IntegrationPatience
  with MockitoSugar
  with IntegrationBase {

  trait ServerBuilderHelper {
    val preparedFactory = mock[ServiceFactory[String, String]]
    val preparedServicePromise = new Promise[Service[String, String]]
    when(preparedFactory.status) thenReturn Status.Open
    when(preparedFactory()) thenReturn preparedServicePromise
    when(preparedFactory.close(any[Time])) thenReturn Future.Done
    when(preparedFactory.map(Matchers.any())) thenReturn
      preparedFactory.asInstanceOf[ServiceFactory[Any, Nothing]]

    val m = new MockChannel
    when(m.codec.prepareConnFactory(any[ServiceFactory[String, String]])) thenReturn preparedFactory
  }

  val svc: Service[String, String] = Service.const(Future.value("hi"))

  def verifyProtocolRegistry(name: String, expected: String)(build: => Server) = {
    test(name + " registers protocol library") {
      val simple = new SimpleRegistry()
      GlobalRegistry.withRegistry(simple) {
        val server = build

        val entries = GlobalRegistry.get.toSet
        val unspecified = entries.count(_.key.startsWith(Seq("server", "not-specified")))
        assert(unspecified == 0, "saw registry keys with 'not-specified' protocol")
        val specified = entries.count(_.key.startsWith(Seq("server", expected)))
        assert(specified > 0, "did not see expected protocol registry keys")
        server.close()
      }
    }
  }

  def verifyServerBoundAddress(name: String, expected: String)(build: => Server) = {
    test(s"$name registers server with bound address") {
      val simple = new SimpleRegistry()
      GlobalRegistry.withRegistry(simple) {
        val server = build

        val entries = GlobalRegistry.get.toSet
        val specified = entries.filter(_.key.startsWith(Seq("server", expected)))
        // Entries are in the form: Entry(List(server, fancy, test, /127.0.0.1:58904, RequestStats, unit),MILLISECONDS)
        val entry = specified.head // data is repeated as entry.key, just take the first
        val hostAndPort = entry.key.filter(_.contains("127.0.0.1")).head
        assert(!hostAndPort.contains(":0"), "unbounded address in server registry")
        server.close()
      }
    }
  }

  def loopback = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)

  verifyProtocolRegistry("#codec(Codec)", expected = "fancy") {
    val ctx = new ServerBuilderHelper {}
    when(ctx.m.codec.protocolLibraryName).thenReturn("fancy")

    ServerBuilder()
      .name("test")
      .codec(ctx.m.codec)
      .bindTo(loopback)
      .build(svc)
  }

  verifyProtocolRegistry("#codec(CodecFactory)", expected = "fancy") {
    val ctx = new ServerBuilderHelper {}
    val cf = new CodecFactory[String, String] {
      def client: Client = ???
      def server: Server = (_: ServerCodecConfig) => ctx.m.codec
      override def protocolLibraryName = "fancy"
    }

    ServerBuilder()
      .name("test")
      .codec(cf)
      .bindTo(loopback)
      .build(svc)
  }

  verifyProtocolRegistry("#codec(CodecFactory#Server)", expected = "fancy") {
    val ctx = new ServerBuilderHelper {}
    when(ctx.m.codec.protocolLibraryName).thenReturn("fancy")

    val cfServer: CodecFactory[String, String]#Server =
      { (_: ServerCodecConfig) => ctx.m.codec }

    ServerBuilder()
      .name("test")
      .codec(cfServer)
      .bindTo(loopback)
      .build(svc)
  }

  verifyProtocolRegistry("#codec(CodecFactory#Server)FancyCodec", expected = "fancy") {
    class FancyCodec extends CodecFactory[String, String] {
      def client = { config =>
       new com.twitter.finagle.Codec[String, String] {
         def pipelineFactory = null
       }
      }

      def server = { config =>
        new com.twitter.finagle.Codec[String, String] {
         def pipelineFactory = null
       }
      }
      override val protocolLibraryName: String = "fancy"
    }
    ServerBuilder()
      .codec(new FancyCodec)
      .bindTo(loopback)
      .name("test")
      .build(svc)
  }

  verifyServerBoundAddress("#codec(CodecFactory#Server)FancyCodec", expected = "fancy") {
    class FancyCodec extends CodecFactory[String, String] {
      def client = { config =>
       new com.twitter.finagle.Codec[String, String] {
         def pipelineFactory = null
       }
      }

      def server = { config =>
        new com.twitter.finagle.Codec[String, String] {
         def pipelineFactory = null
       }
      }
      override val protocolLibraryName: String = "fancy"
    }
    ServerBuilder()
      .codec(new FancyCodec)
      .bindTo(loopback) // loopback is configured to port 0
      .name("test")
      .build(svc)
  }
}
