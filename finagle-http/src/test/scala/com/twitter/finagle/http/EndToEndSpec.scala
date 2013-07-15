package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.util.{Await, Future}
import java.net.InetSocketAddress
import org.specs.SpecificationWithJUnit

class EndToEndSpec extends SpecificationWithJUnit {
  "Echo Server and Client with RichHttp codec" should {
    val echoService = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response(request)
        response.contentString = request.uri
        Future.value(response)
      }
    }

    val server = ServerBuilder()
      .codec(RichHttp[Request](Http()))
      .bindTo(new InetSocketAddress(0))
      .name("server")
      .build(echoService)

    val client = ClientBuilder()
      .codec(RichHttp[Request](Http()))
      .hosts(Seq(server.localAddress))
      .hostConnectionLimit(1)
      .name("client")
      .build()

    doAfter {
      client.close()
      server.close()
    }

    "return same content in response as in request" in {
      val response = client(Request("123"))
      Await.result(response).contentString must_==("123")
    }
  }
}
