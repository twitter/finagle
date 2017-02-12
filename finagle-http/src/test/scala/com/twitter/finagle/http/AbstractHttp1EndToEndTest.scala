package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.io.Buf
import com.twitter.io.Reader.ReaderDiscarded
import com.twitter.util.{Future, Promise, Return, Throw}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

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
      eventually { assert(statsRecv.gauges(Seq("client", "connections"))() == 0.0f) }
      eventually { assert(statsRecv.gauges(Seq("server", "connections"))() == 0.0f) }

      await(client.close())
    }

    // This is a similar to a test in AbstractEndToEndTest, but checks the status of
    // the connection in a manner that is specific to HTTP/1.x
    test(prefix + ": closes the connection on request header fields too large") {
      val service = Service.mk{ _: Request => Future.value(Response()) }

      val client = connect(service)
      val request = Request("/")
      request.headerMap.add("header", "a" * 8192)
      val response = await(client(request))

      assert(response.status == Status.RequestHeaderFieldsTooLarge)
      assert(response.headerMap.get(Fields.Connection) == Some("close"))

      // connections must be closed
      eventually { assert(statsRecv.gauges(Seq("client", "connections"))() == 0.0f) }
      eventually { assert(statsRecv.gauges(Seq("server", "connections"))() == 0.0f) }

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
    eventually { assert(statsRecv.gauges(Seq("client", "connections"))() == 0.0f) }
    eventually { assert(statsRecv.gauges(Seq("server", "connections"))() == 0.0f) }
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

  // HEAD related tests
  test("mishandled HEAD request doesn't foul the connection") {
    val responseString = "a response"
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = responseString
        Future.value(response)
      }
    }
    val server = serverImpl()
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    assert(await(client(Request(Method.Head, "/"))).contentString.isEmpty())
    // Make sure subsequent requests work
    assert(await(client(Request(Method.Get, "/"))).contentString == responseString)
    await(client.close())
    await(server.close())
  }

  test("mishandled HEAD request with chunked response doesn't foul the connection") {
    val writerFinished = new AtomicBoolean(false)
    val responseString = "a response"
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.setChunked(true)
        val writer = response.writer
        val f = for {
          _ <- writer.write(Buf.Utf8(responseString))
          _ <- writer.close()
        } yield ()

        f.respond {
          case Return(_) | Throw(_: ReaderDiscarded) => writerFinished.set(true) // must finish
          case _ => ()
        }

        Future.value(response)
      }
    }
    val server = serverImpl()
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    assert(await(client(Request(Method.Head, "/"))).contentString.isEmpty())
    // Ensure that the writer was closed
    eventually(assert(writerFinished.get()))
    // Make sure subsequent requests work
    assert(await(client(Request(Method.Get, "/"))).contentString == responseString)
    await(client.close())
    await(server.close())
  }

  run(multiplePipelines(implName, _))(nonStreamingConnect)
  run(multiplePipelines(implName + "(streaming)", _))(streamingConnect)

  def multiplePipelines(prefix: String, connect: HttpService => HttpService): Unit = {
    test(prefix + ": Can initialize multiple pipelines") {
      val srvc = Service.mk { req: Request =>
        val resp = Response()
        resp.headerMap.set(Fields.Connection, "close")
        Future.value(resp)
      }

      val client = connect(srvc)

      val resp1 = await(client(Request(uri = "/close")))
      assert(resp1.headerMap.get(Fields.Connection) == Some("close"))

      // The previous request should have terminated the connection so this
      // second request will open a new one.
      val resp2 = await(client(Request(uri = "/stayopen")))
      assert(resp2.headerMap.get(Fields.Connection) == Some("close"))

      await(client.close())
    }
  }
}
