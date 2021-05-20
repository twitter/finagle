package com.twitter.finagle.netty4.ssl

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Address, Service}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import com.twitter.finagle.netty4.ssl.Netty4SslTestComponents._
import com.twitter.util.{Await, Future}
import javax.net.ssl.SSLSession
import org.scalatest.funsuite.AnyFunSuite

class Netty4SslAddressTest extends AnyFunSuite {

  // Timeout for blocking calls
  private[this] val timeout = 15.seconds

  private[this] val echoService = new Service[String, String] {
    def apply(request: String): Future[String] = Future.value(request)
  }

  private[this] val hasAddressSessionVerifier = new SslServerSessionVerifier {
    def apply(address: Address, config: SslServerConfiguration, session: SSLSession): Boolean =
      address match {
        case Address.Inet(isa, _) => !isa.isUnresolved
        case _ => false
      }
  }

  test("SslServerSessionVerifier gets the correct peer address") {
    val server = mkTlsServer(echoService, sessionVerifier = hasAddressSessionVerifier)
    val client = mkTlsClient(getPort(server))

    val response = Await.result(client("hello"), timeout)
    assert(response == "hello")

    Await.result(client.close(), timeout)
    Await.result(server.close(), timeout)
  }

}
