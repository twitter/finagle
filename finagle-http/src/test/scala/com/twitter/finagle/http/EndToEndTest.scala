package com.twitter.finagle.http

import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.{Dtab, Service, ServiceProxy}
import com.twitter.util.{Await, Closable, Future, Time}
import java.io.{StringWriter, PrintWriter}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {
  type HttpService = Service[HttpRequest, HttpResponse]

  def go(name: String)(connect: HttpService => HttpService) {
    test(name + ": echo") {
      val service = new HttpService {
        def apply(request: HttpRequest) = {
          val response = Response(request)
          response.contentString = Request(request).uri
          Future.value(response)
        }
      }

      val client = connect(service)
      val response = client(Request("123"))
      assert(Response(Await.result(response)).contentString === "123")
      client.close()
    }

    test(name + ": dtab") {
      val service = new HttpService {
        def apply(request: HttpRequest) = {
          val stringer = new StringWriter
          val printer = new PrintWriter(stringer)
          Dtab.baseDiff().print(printer)
          val response = Response(request)
          response.contentString = stringer.toString
          Future.value(response)
        }
      }

      val client = connect(service)

      Dtab.unwind {
        Dtab.delegate("/a", "/b")
        Dtab.delegate("/c", "/d")

        val res = Response(Await.result(client(Request("/"))))
        assert(res.contentString === "Dtab(2)\n\t/a -> /b\n\t/c -> /d\n")
      }

      client.close()
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) {

    go("ClientBuilder") {
      service =>
        val server = ServerBuilder()
          .codec(Http())
          .bindTo(new InetSocketAddress(0))
          .name("server")
          .build(service)

        val client = ClientBuilder()
          .codec(Http())
          .hosts(Seq(server.localAddress))
          .hostConnectionLimit(1)
          .name("client")
          .build()

        new ServiceProxy(client) {
          override def close(deadline: Time) =
            Closable.all(client, server).close(deadline)
        }
    }

    go("Client/Server") {
      service =>
        import com.twitter.finagle.Http
        val server = Http.serve(":*", service)
        val client = Http.newService(server)

        new ServiceProxy(client) {
          override def close(deadline: Time) =
            Closable.all(client, server).close(deadline)
        }
    }
  }

}
