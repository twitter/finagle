package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Future, Promise}
import java.net.InetSocketAddress

abstract class AbstractHttp1EndToEndTest extends AbstractEndToEndTest {

  run(http10Tests(_), initialLineLength(_), connectionClose("non-streaming"))(nonStreamingConnect)
  run(connectionClose("streaming"))(streamingConnect)

  def http10Tests(connect: HttpService => HttpService): Unit = {
    test(implName + ": HTTP/1.0") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response(request.version, Status.Ok))
      }
      val client = connect(service)
      val request = Request(Method.Get, "/http/1.0")
      request.version = Version.Http10
      val response = await(client(request))
      assert(response.status == Status.Ok)
      await(client.close())
    }
  }

  def initialLineLength(connect: HttpService => HttpService): Unit = {
    test(implName + ": initial request line too long") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request("/" + "a" * 4096)
      val response = await(client(request))
      assert(response.status == Status.RequestURITooLong)
      await(client.close())
    }
  }

  /**
   * Tests that the Connection header is utilized correctly to close the underlying
   * socket connection.
   *
   * This should occur in a number of situations:
   * - The request contains a Connection: close header
   * - The request is a HTTP/1.0 request without a Connection: keep-alive header
   * - The response contains a Connection: close header
   */
  def connectionClose(kind: String)(connect: HttpService => HttpService): Unit = {
    val prefix = s"$implName ($kind): "

    test(prefix + "Request with 'Connection: close'") {
      val request = Request()
      request.headerMap("Connection") = "close"
      connectionCloseTest(request, Service.mk(_ => Future.value(Response())))(connect)
    }

    test(prefix + "Response with 'Connection: close'") {
      val service = Service.mk{ req: Request =>
        val resp = Response()
        resp.headerMap.set(Fields.Connection, "close")
        Future.value(resp)
      }
      val client = connect(service)
      val response = await(client(Request()))

      assert(response.headerMap.get(Fields.Connection) == Some("close"))

      // connections must be closed
      assert(statsRecv.gauges(Seq("client", "connections"))() == 0.0f)
      assert(statsRecv.gauges(Seq("server", "connections"))() == 0.0f)

      await(client.close())
    }

    // This is a similar to a test in AbstractEndToEndTest, but checks the status of
    // the connection in a manner that is specific to HTTP/1.x
    test(prefix + ": closes the connection on request header fields too large") {
      val service = Service.mk{ _: Request => Future.value(Response()) }

      val client = connect(service)
      val request = Request("/")
      request.headers().add("header", "a" * 8192)
      val response = await(client(request))

      assert(response.status == Status.RequestHeaderFieldsTooLarge)
      assert(response.headerMap.get(Fields.Connection) == Some("close"))

      // connections must be closed
      assert(statsRecv.gauges(Seq("client", "connections"))() == 0.0f)
      assert(statsRecv.gauges(Seq("server", "connections"))() == 0.0f)

      await(client.close())
    }

    test(prefix + "HTTP/1.0") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response(request.version, Status.Ok))
      }
      val client = connect(service)
      val request = Request(Method.Get, "/http/1.0")
      request.version = Version.Http10
      val response = await(client(request))
      assert(response.status == Status.Ok)
      await(client.close())
    }
  }

  private def connectionCloseTest(request: Request, service: HttpService)(connect: HttpService => HttpService): Unit = {
    val client = connect(service)
    val response = await(client(request))
    assert(response.status == Status.Ok)
    assert(response.headerMap.get(Fields.Connection) == Some("close"))
    assert(statsRecv.gauges(Seq("client", "connections"))() == 0.0f)
    assert(statsRecv.gauges(Seq("server", "connections"))() == 0.0f)
    await(client.close())
  }

  test("server handles expect continue header") {
    val expectP = new Promise[Boolean]

    val svc = new HttpService {
      def apply(request: Request) = {
        expectP.setValue(request.headerMap.contains("expect"))
        val response = Response()
        Future.value(response)
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .withStreaming(true)
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(statsRecv)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val req = Request("/streaming")
    req.setChunked(false)
    req.headerMap.set("expect", "100-continue")

    val res = client(req)
    assert(await(res).status == Status.Continue)
    assert(await(expectP) == false)
    await(client.close())
    await(server.close())
  }
}
