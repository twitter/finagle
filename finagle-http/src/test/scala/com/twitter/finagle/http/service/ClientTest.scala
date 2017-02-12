package com.twitter.finagle.http.service

import com.twitter.conversions.time._
import com.twitter.finagle.{ChannelClosedException, ClientConnection, Http, ServiceFactory}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http => HttpCodec, Request, Response, Version, Method}
import com.twitter.finagle.service.FailingFactory
import com.twitter.util.{Await, Throw, Try}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite {
  def withServer(factory: ServiceFactory[Request, Response])(
    spec: ClientBuilder.Complete[Request, Response] => Unit
  ): Unit = {
    val server = Http.serve(new InetSocketAddress(0), factory)
    val serverAddress = server.boundAddress.asInstanceOf[InetSocketAddress]

    val builder =
      ClientBuilder()
        .hosts(serverAddress)
        .hostConnectionLimit(1)
        .codec(HttpCodec())

    try spec(builder) finally {
      Await.result(server.close())
    }
  }

  var counter = 0

  def failingFactory: ServiceFactory[Request, Response] =
    new FailingFactory[Request, Response](new Exception("bye")) {
      override def apply(conn: ClientConnection) = {
        counter += 1
        super.apply(conn)
      }
    }

  test("report a closed connection when the server doesn't reply") {
    withServer(failingFactory) { clientBuilder =>
      counter = 0
      val client = clientBuilder.build()
      try {
        // No failures have happened yet.
        assert(client.isAvailable == true)
        val future = client(Request(Version.Http11, Method.Get, "/"))
        val resolved = Try(Await.result(future, 1.second))
        assert(resolved.isThrow == true)
        val Throw(cause) = resolved
        intercept[ChannelClosedException] { throw cause }
      } finally client.close()
    }
  }

  test("report a closed connection when the server doesn't reply, without retrying") {
    withServer(failingFactory) { clientBuilder =>
      counter = 0
      val client = clientBuilder
        .retries(10)
        .build()
      try {
        val future = client(Request(Version.Http11, Method.Get, "/"))
        val resolved = Try(Await.result(future, 1.second))
        assert(resolved.isThrow == true)
        val Throw(cause) = resolved
        intercept[ChannelClosedException] { throw cause }
        assert(counter == 1)
      } finally client.close()
    }
  }
}
