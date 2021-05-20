package com.twitter.finagle.http.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Method, Request, Response, Version}
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.{ChannelClosedException, ClientConnection, Http, ServiceFactory}
import com.twitter.util.Await
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class ClientTest extends AnyFunSuite {
  def withServer(
    factory: ServiceFactory[Request, Response]
  )(
    spec: ClientBuilder.Complete[Request, Response] => Unit
  ): Unit = {
    val server = Http.serve(new InetSocketAddress(0), factory)
    val serverAddress = server.boundAddress.asInstanceOf[InetSocketAddress]

    val builder =
      ClientBuilder()
        .hosts(serverAddress)
        .hostConnectionLimit(1)
        .stack(Http.client)

    try spec(builder)
    finally {
      Await.result(server.close())
    }
  }

  @volatile private[this] var counter = 0

  def failingFactory: ServiceFactory[Request, Response] =
    new FailingFactory[Request, Response](new Exception("bye")) {
      override def apply(conn: ClientConnection) = {
        counter += 1
        super.apply(conn)
      }
    }

  if (!sys.props.contains("SKIP_FLAKY_TRAVIS"))
    test("report a closed connection when the server doesn't reply") {
      withServer(failingFactory) { clientBuilder =>
        counter = 0
        val client = clientBuilder.build()
        try {
          // No failures have happened yet.
          assert(client.isAvailable)
          val future = client(Request(Version.Http11, Method.Get, "/"))
          intercept[ChannelClosedException] {
            Await.result(future, 1.second)
          }
        } finally client.close()
      }
    }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("report a closed connection when the server doesn't reply, without retrying") {
      withServer(failingFactory) { clientBuilder =>
        counter = 0
        val client = clientBuilder
          .retries(10)
          .build()
        try {
          val future = client(Request(Version.Http11, Method.Get, "/"))
          intercept[ChannelClosedException] {
            Await.result(future, 1.second)
          }
          assert(counter == 1)
        } finally client.close()
      }
    }
}
