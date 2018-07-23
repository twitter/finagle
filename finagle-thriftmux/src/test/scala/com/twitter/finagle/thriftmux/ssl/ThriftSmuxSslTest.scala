package com.twitter.finagle.thriftmux.ssl

import com.twitter.conversions.time._
import com.twitter.finagle.{Mux, SslException, SslVerificationFailedException}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.toggle.flag
import com.twitter.finagle.thriftmux.ssl.ThriftSmuxSslTestComponents._
import com.twitter.finagle.thriftmux.thriftscala._
import com.twitter.util.{Await, Awaitable}
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import scala.util.control.NonFatal

class ThriftSmuxSslTest extends FunSuite with Eventually {
  
  private def await[T](t: Awaitable[T]): T =
    Await.result(t, 5.seconds)

  private[this] def assertGaugeIsZero(
    statsReceiver: InMemoryStatsReceiver,
    name: Array[String]
  ): Unit = statsReceiver.gauges.get(name) match {
    case Some(f) => assert(f() == 0.0)
    case None => // all good
  }

  private[this] def assertGaugeIsNonZero(value: Float)(
    statsReceiver: InMemoryStatsReceiver,
    name: Array[String]
  ): Unit = statsReceiver.gauges.get(name) match {
    case Some(f) => assert(f() == value)
    case None => fail()
  }

  private[this] val assertGaugeIsOne = assertGaugeIsNonZero(1.0F) _
  private[this] val assertGaugeIsTwo = assertGaugeIsNonZero(2.0F) _

  private[this] def mkSuccessfulHelloRequest(client: TestService.MethodPerEndpoint): Unit = {
    val response = await(client.query("hello"))
    assert(response == "hellohello")
  }

  test("Single client and server results in server TLS connections incremented and decremented") {
    flag.overrides.let(Mux.param.MuxImpl.TlsHeadersToggleId, 1.0) {
      val serverStats = new InMemoryStatsReceiver
      val serverTlsConnections = Array("server", "tls", "connections")

      val server = mkTlsServer("server", serverStats)
      val client = mkTlsClient(getPort(server))

      assertGaugeIsZero(serverStats, serverTlsConnections)
      mkSuccessfulHelloRequest(client)
      assertGaugeIsOne(serverStats, serverTlsConnections)

      Await.ready(client.asClosable.close())
      eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
      await(server.close())
    }
  }

  test("Single client and server results in client TLS connections incremented and decremented") {
    flag.overrides.let(Mux.param.MuxImpl.TlsHeadersToggleId, 1.0) {
      val clientStats = new InMemoryStatsReceiver
      val clientTlsConnections = Array("client", "tls", "connections")

      val server = mkTlsServer()
      val client = mkTlsClient(getPort(server), "client", clientStats)

      assertGaugeIsZero(clientStats, clientTlsConnections)
      mkSuccessfulHelloRequest(client)
      assertGaugeIsOne(clientStats, clientTlsConnections)

      Await.ready(client.asClosable.close())
      eventually { assertGaugeIsZero(clientStats, clientTlsConnections) }
      await(server.close())
    }
  }

  def doWrapped[T](a: => T): T = {
    try a
    catch {
      case NonFatal(t) => throw new Exception("Caught exception", t)
    }
  }

  test("Multiple clients and server results in server TLS connections incremented and decremented") {
    flag.overrides.let(Mux.param.MuxImpl.TlsHeadersToggleId, 1.0) {
      val serverStats = new InMemoryStatsReceiver

      val server = mkTlsServer("server", serverStats)
      val serverPort = getPort(server)
      val client1 = mkTlsClient(serverPort, "client1")
      val client2 = mkTlsClient(serverPort, "client2")

      val serverTlsConnections = Array("server", "tls", "connections")

      doWrapped { assertGaugeIsZero(serverStats, serverTlsConnections) }
      doWrapped { mkSuccessfulHelloRequest(client1) }
      doWrapped { assertGaugeIsOne(serverStats, serverTlsConnections) }

      doWrapped { mkSuccessfulHelloRequest(client2) }
      doWrapped { assertGaugeIsTwo(serverStats, serverTlsConnections) }

      doWrapped { await(client1.asClosable.close()) }
      doWrapped { eventually { assertGaugeIsOne(serverStats, serverTlsConnections) } }

      doWrapped { await(client2.asClosable.close()) }
      doWrapped { await(server.close()) }
      doWrapped { eventually { assertGaugeIsZero(serverStats, serverTlsConnections) } }
    }
  }

  test("Multiple clients and server results in separate client TLS connections incremented and decremented") {
    flag.overrides.let(Mux.param.MuxImpl.TlsHeadersToggleId, 1.0) {
      val client1Stats = new InMemoryStatsReceiver
      val client2Stats = new InMemoryStatsReceiver

      val server = mkTlsServer("server")
      val serverPort = getPort(server)
      val client1 = mkTlsClient(serverPort, "client1", client1Stats)
      val client2 = mkTlsClient(serverPort, "client2", client2Stats)

      val client1TlsConnections = Array("client1", "tls", "connections")
      val client2TlsConnections = Array("client2", "tls", "connections")

      assertGaugeIsZero(client1Stats, client1TlsConnections)
      assertGaugeIsZero(client2Stats, client2TlsConnections)

      mkSuccessfulHelloRequest(client1)
      assertGaugeIsOne(client1Stats, client1TlsConnections)
      assertGaugeIsZero(client2Stats, client2TlsConnections)

      mkSuccessfulHelloRequest(client2)
      assertGaugeIsOne(client1Stats, client1TlsConnections)
      assertGaugeIsOne(client2Stats, client2TlsConnections)

      await(client1.asClosable.close())
      eventually {
        assertGaugeIsZero(client1Stats, client1TlsConnections)
        assertGaugeIsOne(client2Stats, client2TlsConnections)
      }

      await(client2.asClosable.close())
      await(server.close())
      eventually {
        assertGaugeIsZero(client1Stats, client1TlsConnections)
        assertGaugeIsZero(client2Stats, client2TlsConnections)
      }
    }
  }

  test("Single client and rejecting server results in both sides TLS connections at 0") {
    flag.overrides.let(Mux.param.MuxImpl.TlsHeadersToggleId, 1.0) {
      val serverStats = new InMemoryStatsReceiver
      val serverTlsConnections = Array("server", "tls", "connections")

      val clientStats = new InMemoryStatsReceiver
      val clientTlsConnections = Array("client", "tls", "connections")

      val server = mkTlsServer("server", serverStats, NeverValidServerSide)
      val client = mkTlsClient(getPort(server), "client", clientStats)

      assertGaugeIsZero(serverStats, serverTlsConnections)
      assertGaugeIsZero(clientStats, clientTlsConnections)

      intercept[SslException] {
        await(client.query("hello"))
      }

      eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
      eventually { assertGaugeIsZero(clientStats, clientTlsConnections) }

      Await.ready(client.asClosable.close())
      await(server.close())

      eventually {
        assertGaugeIsZero(serverStats, serverTlsConnections)
        assertGaugeIsZero(clientStats, clientTlsConnections)
      }
    }
  }

  test("Single rejecting client and server results in both sides TLS connections at 0") {
    flag.overrides.let(Mux.param.MuxImpl.TlsHeadersToggleId, 1.0) {
      val serverStats = new InMemoryStatsReceiver
      val serverTlsConnections = Array("server", "tls", "connections")

      val clientStats = new InMemoryStatsReceiver
      val clientTlsConnections = Array("client", "tls", "connections")

      val server = mkTlsServer("server", serverStats)
      val client = mkTlsClient(getPort(server), "client", clientStats, NeverValidClientSide)

      assertGaugeIsZero(serverStats, serverTlsConnections)
      assertGaugeIsZero(clientStats, clientTlsConnections)

      intercept[SslVerificationFailedException] {
        await(client.query("hello"))
      }

      eventually {
        assertGaugeIsZero(serverStats, serverTlsConnections)
        assertGaugeIsZero(clientStats, clientTlsConnections)
      }

      Await.ready(client.asClosable.close())
      await(server.close())

      eventually {
        assertGaugeIsZero(serverStats, serverTlsConnections)
        assertGaugeIsZero(clientStats, clientTlsConnections)
      }
    }
  }
}
