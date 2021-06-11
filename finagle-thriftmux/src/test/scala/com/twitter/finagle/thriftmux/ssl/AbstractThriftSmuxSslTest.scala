package com.twitter.finagle.thriftmux.ssl

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.ssl.client.SslClientSessionVerifier
import com.twitter.finagle.ssl.server.SslServerSessionVerifier
import com.twitter.finagle.{ChannelClosedException, ListeningServer, SslVerificationFailedException}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.thriftmux.ssl.ThriftSmuxSslTestComponents._
import com.twitter.finagle.thriftmux.thriftscala._
import com.twitter.util.{Await, Awaitable, Closable, Duration}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.concurrent.Eventually

abstract class AbstractThriftSmuxSslTest extends AnyFunSuite with Eventually {

  protected def doMkTlsServer(
    label: String = "server",
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sessionVerifier: SslServerSessionVerifier = SslServerSessionVerifier.AlwaysValid
  ): ListeningServer

  protected def doMkTlsClient(
    port: Int,
    label: String = "client",
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sessionVerifier: SslClientSessionVerifier = SslClientSessionVerifier.AlwaysValid
  ): TestService.MethodPerEndpoint

  // this method is a safety-net to ensure that resources always get cleaned up, even if
  // an unexpected exception gets thrown
  private[this] def tryWithResources[T](closables: Closable*)(fn: => T): T =
    try {
      fn
    } finally {
      await(Closable.all(closables: _*).close())
    }

  private[this] def assertGaugeIsZero(
    statsReceiver: InMemoryStatsReceiver,
    name: Array[String]
  ): Unit = statsReceiver.gauges.get(name) match {
    case Some(f) => assert(f() == 0.0)
    case None => // all good
  }

  private[this] def assertGaugeIsNonZero(
    value: Float
  )(
    statsReceiver: InMemoryStatsReceiver,
    name: Array[String]
  ): Unit = statsReceiver.gauges.get(name) match {
    case Some(f) => assert(f() == value)
    case None => fail()
  }

  def await[T](a: Awaitable[T], d: Duration = 2.seconds): T =
    Await.result(a, d)

  private[this] val assertGaugeIsOne = assertGaugeIsNonZero(1.0f) _
  private[this] val assertGaugeIsTwo = assertGaugeIsNonZero(2.0f) _

  private[this] def mkSuccessfulHelloRequest(client: TestService.MethodPerEndpoint): Unit = {
    val response = await(client.query("hello"))
    assert(response == "hellohello")
  }

  test("Single client and server results in server TLS connections incremented and decremented") {
    val serverStats = new InMemoryStatsReceiver
    val serverTlsConnections = Array("server", "tls", "connections")

    val server = doMkTlsServer("server", serverStats)
    val client = doMkTlsClient(getPort(server))

    tryWithResources(server, client.asClosable) {
      assertGaugeIsZero(serverStats, serverTlsConnections)
      mkSuccessfulHelloRequest(client)
      assertGaugeIsOne(serverStats, serverTlsConnections)

      await(client.asClosable.close())
      eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
      await(server.close())
    }
  }

  test("Single client and server results in client TLS connections incremented and decremented") {
    val clientStats = new InMemoryStatsReceiver
    val clientTlsConnections = Array("client", "tls", "connections")

    val server = doMkTlsServer()
    val client = doMkTlsClient(getPort(server), "client", clientStats)

    tryWithResources(server, client.asClosable) {
      assertGaugeIsZero(clientStats, clientTlsConnections)
      mkSuccessfulHelloRequest(client)
      assertGaugeIsOne(clientStats, clientTlsConnections)

      await(client.asClosable.close())
      eventually { assertGaugeIsZero(clientStats, clientTlsConnections) }
      await(server.close())
    }

  }

  test(
    "Multiple clients and server results in server TLS connections incremented and decremented") {
    val serverStats = new InMemoryStatsReceiver

    val server = doMkTlsServer("server", serverStats)
    val serverPort = getPort(server)
    val client1 = doMkTlsClient(serverPort, "client1")
    val client2 = doMkTlsClient(serverPort, "client2")

    tryWithResources(server, client1.asClosable, client2.asClosable) {
      val serverTlsConnections = Array("server", "tls", "connections")

      assertGaugeIsZero(serverStats, serverTlsConnections)
      mkSuccessfulHelloRequest(client1)
      assertGaugeIsOne(serverStats, serverTlsConnections)

      mkSuccessfulHelloRequest(client2)
      assertGaugeIsTwo(serverStats, serverTlsConnections)

      await(client1.asClosable.close())
      eventually { assertGaugeIsOne(serverStats, serverTlsConnections) }

      await(client2.asClosable.close())
      await(server.close())
      eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
    }

  }

  test(
    "Multiple clients and server results in separate client TLS connections incremented and decremented"
  ) {
    val client1Stats = new InMemoryStatsReceiver
    val client2Stats = new InMemoryStatsReceiver

    val server = doMkTlsServer("server")
    val serverPort = getPort(server)
    val client1 = doMkTlsClient(serverPort, "client1", client1Stats)
    val client2 = doMkTlsClient(serverPort, "client2", client2Stats)

    tryWithResources(server, client1.asClosable, client2.asClosable) {
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
    val serverStats = new InMemoryStatsReceiver
    val serverTlsConnections = Array("server", "tls", "connections")

    val clientStats = new InMemoryStatsReceiver
    val clientTlsConnections = Array("client", "tls", "connections")

    val server = doMkTlsServer("server", serverStats, NeverValidServerSide)
    val client = doMkTlsClient(getPort(server), "client", clientStats)

    tryWithResources(server, client.asClosable) {
      assertGaugeIsZero(serverStats, serverTlsConnections)
      assertGaugeIsZero(clientStats, clientTlsConnections)

      // If the server rejects the handshake, it just hangs up. Therefore,
      // we expect to get a ChannelClosedException here.
      val e = intercept[ChannelClosedException] {
        await(client.query("hello"))
      }
      // await(client.query("hello"))
      e match {
        case x: com.twitter.finagle.UnknownChannelException => println(x.toString); throw x 
        case x: com.twitter.finagle.Failure => println(x.toString); throw x
        case _ => // nothing
      }

      eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
      eventually { assertGaugeIsZero(clientStats, clientTlsConnections) }

      await(client.asClosable.close())
      await(server.close())

      eventually {
        assertGaugeIsZero(serverStats, serverTlsConnections)
        assertGaugeIsZero(clientStats, clientTlsConnections)
      }
    }
  }

  test("Single rejecting client and server results in both sides TLS connections at 0") {
    val serverStats = new InMemoryStatsReceiver
    val serverTlsConnections = Array("server", "tls", "connections")

    val clientStats = new InMemoryStatsReceiver
    val clientTlsConnections = Array("client", "tls", "connections")

    val server = doMkTlsServer("server", serverStats)
    val client = doMkTlsClient(getPort(server), "client", clientStats, NeverValidClientSide)

    tryWithResources(server, client.asClosable) {
      assertGaugeIsZero(serverStats, serverTlsConnections)
      assertGaugeIsZero(clientStats, clientTlsConnections)

      val exn = intercept[Throwable] {
        await(client.query("hello"))
      }

      // We need to make sure we failed due to the ssl verification
      def findSslVerificationFailedException(ex: Throwable): Unit = ex match {
        case null => throw new Exception("Failed to find SslVerficationFailure", exn)
        case _: SslVerificationFailedException => // ok
        case ex => findSslVerificationFailedException(ex.getCause)
      }
      findSslVerificationFailedException(exn)

      eventually {
        assertGaugeIsZero(serverStats, serverTlsConnections)
        assertGaugeIsZero(clientStats, clientTlsConnections)
      }

      await(client.asClosable.close())
      await(server.close())

      eventually {
        assertGaugeIsZero(serverStats, serverTlsConnections)
        assertGaugeIsZero(clientStats, clientTlsConnections)
      }
    }
  }
}
