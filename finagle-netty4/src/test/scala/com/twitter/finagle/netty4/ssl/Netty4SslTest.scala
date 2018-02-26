package com.twitter.finagle.netty4.ssl

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.netty4.ssl.Netty4SslTestComponents._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Future}
import org.scalatest.FunSuite

class Netty4SslTest extends FunSuite {

  private[this] val peerCertService = new Service[String, String] {
    override def apply(request: String): Future[String] = {
      Future.value(Transport.peerCertificate.isDefined match {
        case true => "OK"
        case false => "ERROR"
      })
    }
  }

  test("Peer certificate is available to service") {
    val server = mkTlsServer(peerCertService)
    val client = mkTlsClient(getPort(server))

    val response = Await.result(client("peer cert test"), 2.seconds)
    assert(response == "OK")

    Await.result(client.close(), 2.seconds)
    Await.result(server.close(), 2.seconds)
  }

}
