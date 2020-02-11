package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Address, Name}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Future
import java.net.InetSocketAddress

// Adds some tests that are specific to the HTTP/2 transports
abstract class AbstractHttp2EndToEndTest extends AbstractEndToEndTest {

  test(s"$implName: HTTP/2 session idle times don't bork h2 sessions") {
    val service = new HttpService {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = request.uri
        Future.value(response).delayed(200.milliseconds)(DefaultTimer.Implicit)
      }
    }

    val server = serverImpl().withSession
      .maxIdleTime(300.milliseconds)
      .serve(new InetSocketAddress(0), service)

    val client = clientImpl().newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    await(client(Request("/1")))
    await(client(Request("/2")))
    await(server.close())
    await(client.close())
  }

  test("client closes properly when closed") {

    val server = serverImpl()
      .serve("localhost:*", initService)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    initClient(client)

    await(client(Request(Method.Get, "/")))
    await(client.close())

    // For some reason the upgrading client closes a bunch of times which makes
    // the counter kind of meaningless quantitatively but we need to close at
    // least one time.
    eventually {
      assert(statsRecv.counters(Seq("client", "closes")) > 0)
    }
    await(server.close())
  }

  test("client doesn't honor the Netty stream dependency extension header") {
    val server = serverImpl().serve("localhost:*", initService)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl().newService(s"${addr.getHostName}:${addr.getPort}", "client")

    initClient(client)

    val req = Request(Method.Get, "/")
    req.headerMap.add("x-http2-stream-dependency-id", "1")

    try await(client(req))
    finally {
      await(client.close())
      await(server.close())
    }
  }
}
