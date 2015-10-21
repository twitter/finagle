package com.twitter.finagle.builder

import java.net.{InetSocketAddress, InetAddress}

import com.twitter.finagle._
import com.twitter.finagle.integration.IntegrationBase
import com.twitter.finagle.service.FailureAccrualFactory
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
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
}
