package com.twitter.finagle.netty4.ssl

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.netty4.ssl.Netty4SslTestComponents._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{ChannelClosedException, Failure, Service}
import com.twitter.util.{Await, Future}
import com.twitter.util.{Return, Throw, Try}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite

class Netty4SslTest extends AnyFunSuite with Eventually with IntegrationPatience {

  // Timeout for blocking calls
  private val timeout = 15.seconds

  private[this] val worldService = new Service[String, String] {
    override def apply(request: String): Future[String] = Future.value("world")
  }

  private[this] val peerCertService = new Service[String, String] {
    override def apply(request: String): Future[String] = {
      Future.value(Transport.peerCertificate.isDefined match {
        case true => "OK"
        case false => "ERROR"
      })
    }
  }

  private[this] def assertGaugeIsZero(
    statsReceiver: InMemoryStatsReceiver,
    name: Array[String]
  ): Unit = statsReceiver.gauges.get(name.toIndexedSeq) match {
    case Some(f) => assert(f() == 0.0)
    case None => // all good
  }

  private[this] def assertGaugeIsNonZero(
    value: Float
  )(
    statsReceiver: InMemoryStatsReceiver,
    name: Array[String]
  ): Unit = statsReceiver.gauges.get(name.toIndexedSeq) match {
    case Some(f) => assert(f() == value)
    case None => fail()
  }

  private[this] val assertGaugeIsOne = assertGaugeIsNonZero(1.0f) _
  private[this] val assertGaugeIsTwo = assertGaugeIsNonZero(2.0f) _

  private[this] def mkSuccessfulHelloRequest(client: Service[String, String]): Unit = {
    val response = Await.result(client("hello"), timeout)
    assert(response == "world")
  }

  test("Peer certificate is available to service") {
    val server = mkTlsServer(peerCertService)
    val client = mkTlsClient(getPort(server))

    val response = Await.result(client("peer cert test"), timeout)
    assert(response == "OK")

    Await.result(client.close(), timeout)
    Await.result(server.close(), timeout)
  }

  test("Single client and server results in server TLS connections incremented and decremented") {
    val serverStats = new InMemoryStatsReceiver
    val serverTlsConnections = Array("server", "tls", "connections")

    val server = mkTlsServer(worldService, "server", serverStats)
    val client = mkTlsClient(getPort(server))

    assertGaugeIsZero(serverStats, serverTlsConnections)
    mkSuccessfulHelloRequest(client)
    assertGaugeIsOne(serverStats, serverTlsConnections)

    Await.ready(client.close(), timeout)
    eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
    Await.result(server.close(), timeout)
  }

  test("Single client and server results in client TLS connections incremented and decremented") {
    val clientStats = new InMemoryStatsReceiver
    val clientTlsConnections = Array("client", "tls", "connections")

    val server = mkTlsServer(worldService)
    val client = mkTlsClient(getPort(server), "client", clientStats)

    assertGaugeIsZero(clientStats, clientTlsConnections)
    mkSuccessfulHelloRequest(client)
    assertGaugeIsOne(clientStats, clientTlsConnections)

    Await.ready(client.close(), timeout)
    eventually { assertGaugeIsZero(clientStats, clientTlsConnections) }
    Await.result(server.close(), timeout)
  }

  test(
    "Multiple clients and server results in server TLS connections incremented and decremented") {
    val serverStats = new InMemoryStatsReceiver

    val server = mkTlsServer(worldService, "server", serverStats)
    val serverPort = getPort(server)
    val client1 = mkTlsClient(serverPort, "client1")
    val client2 = mkTlsClient(serverPort, "client2")

    val serverTlsConnections = Array("server", "tls", "connections")

    assertGaugeIsZero(serverStats, serverTlsConnections)
    mkSuccessfulHelloRequest(client1)
    assertGaugeIsOne(serverStats, serverTlsConnections)

    mkSuccessfulHelloRequest(client2)
    assertGaugeIsTwo(serverStats, serverTlsConnections)

    Await.result(client1.close(), timeout)
    eventually { assertGaugeIsOne(serverStats, serverTlsConnections) }

    Await.result(client2.close(), timeout)
    Await.result(server.close(), timeout)
    eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
  }

  test(
    "Multiple clients and server results in separate client TLS connections incremented and decremented"
  ) {
    val client1Stats = new InMemoryStatsReceiver
    val client2Stats = new InMemoryStatsReceiver

    val server = mkTlsServer(worldService, "server")
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

    Await.result(client1.close(), timeout)
    eventually {
      assertGaugeIsZero(client1Stats, client1TlsConnections)
      assertGaugeIsOne(client2Stats, client2TlsConnections)
    }

    Await.result(client2.close(), timeout)
    Await.result(server.close(), timeout)
    eventually {
      assertGaugeIsZero(client1Stats, client1TlsConnections)
      assertGaugeIsZero(client2Stats, client2TlsConnections)
    }
  }

  test("Single client and rejecting server results in both sides TLS connections at 0") {
    val serverStats = new InMemoryStatsReceiver
    val serverTlsConnections = Array("server", "tls", "connections")

    val clientStats = new InMemoryStatsReceiver
    val clientTlsConnections = Array("client", "tls", "connections")

    val server = mkTlsServer(worldService, "server", serverStats, NeverValidServerSide)
    val client = mkTlsClient(getPort(server), "client", clientStats)

    assertGaugeIsZero(serverStats, serverTlsConnections)
    assertGaugeIsZero(clientStats, clientTlsConnections)

    intercept[ChannelClosedException] {
      Await.result(client("hello"), timeout)
    }

    eventually { assertGaugeIsZero(serverStats, serverTlsConnections) }
    eventually { assertGaugeIsZero(clientStats, clientTlsConnections) }

    Await.ready(client.close(), timeout)
    Await.result(server.close(), timeout)

    eventually {
      assertGaugeIsZero(serverStats, serverTlsConnections)
      assertGaugeIsZero(clientStats, clientTlsConnections)
    }
  }

  test("Single rejecting client and server results in both sides TLS connections at 0") {
    val serverStats = new InMemoryStatsReceiver
    val serverTlsConnections = Array("server", "tls", "connections")

    val clientStats = new InMemoryStatsReceiver
    val clientTlsConnections = Array("client", "tls", "connections")

    val server = mkTlsServer(worldService, "server", serverStats)
    val client = mkTlsClient(getPort(server), "client", clientStats, NeverValidClientSide)

    assertGaugeIsZero(serverStats, serverTlsConnections)
    assertGaugeIsZero(clientStats, clientTlsConnections)

    intercept[Failure] {
      Await.result(client("hello"), timeout)
    }

    eventually {
      assertGaugeIsZero(serverStats, serverTlsConnections)
      assertGaugeIsZero(clientStats, clientTlsConnections)
    }

    Await.ready(client.close(), timeout)
    Await.result(server.close(), timeout)

    eventually {
      assertGaugeIsZero(serverStats, serverTlsConnections)
      assertGaugeIsZero(clientStats, clientTlsConnections)
    }
  }

  test("ssl handshake complete callback") {
    val server = mkTlsServer(peerCertService)

    @volatile var handshakeSuccessful = false
    val callback: Try[Unit] => Unit = {
      case Return(_) => handshakeSuccessful = true
      case Throw(_) => handshakeSuccessful = false
    }
    val client = mkTlsClient(getPort(server), onHandshakeComplete = callback)

    val response = Await.result(client("peer cert test"), timeout)
    assert(response == "OK")
    assert(handshakeSuccessful)

    Await.result(client.close(), timeout)
    Await.result(server.close(), timeout)
  }
}
