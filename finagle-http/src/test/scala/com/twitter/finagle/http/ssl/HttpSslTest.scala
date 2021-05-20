package com.twitter.finagle.http.ssl

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.http.ssl.HttpSslTestComponents._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Awaitable, Duration, Future}
import org.scalatest.funsuite.AnyFunSuite

class HttpSslTest extends AnyFunSuite {

  private[this] def await[T](f: Awaitable[T]): T =
    Await.result(f, Duration.fromSeconds(5))

  private val service = new Service[Request, Response] {
    override def apply(request: Request): Future[Response] = {
      Transport.peerCertificate match {
        case Some(_) =>
          val response = Response()
          response.status = Status.Ok
          response.contentString = "OK"
          Future.value(response)
        case None =>
          val response = Response()
          response.status = Status.BadRequest
          response.contentString = "NOPE"
          Future.value(response)
      }
    }
  }

  test("Peer certificate is available to HTTP/1.1 service") {
    val server = mkTlsServer(useHttp2 = false, service = service)
    val client = mkTlsClient(useHttp2 = false, port = getPort(server))

    try {
      val request = Request("/")
      val response = await(client(request))
      assert(response.status == Status.Ok)
    } finally {
      await(client.close())
      await(server.close())
    }
  }

  test("Peer certificate is available to HTTP/2 service") {
    val server = mkTlsServer(useHttp2 = true, service = service)
    val client = mkTlsClient(useHttp2 = true, port = getPort(server))

    try {
      val request = Request("/")
      val response = await(client(request))
      assert(response.status == Status.Ok)
    } finally {
      await(client.close())
      await(server.close())
    }
  }

  test("Peer certificate is available to HTTP/2 service with HTTP/1.1 client") {
    val server = mkTlsServer(useHttp2 = true, service = service)
    val client = mkTlsClient(useHttp2 = false, port = getPort(server))

    try {
      val request = Request("/")
      val response = await(client(request))
      assert(response.status == Status.Ok)
    } finally {
      await(client.close())
      await(server.close())
    }
  }

}
