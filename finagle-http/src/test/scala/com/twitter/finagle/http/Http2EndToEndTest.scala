package com.twitter.finagle.http

import com.twitter.conversions.storage._
import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Http2EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4 http/2"
  def clientImpl(): finagle.Http.Client = finagle.Http.client.configuredParams(finagle.Http.Http2)

  def serverImpl(): finagle.Http.Server = finagle.Http.server.configuredParams(finagle.Http.Http2)

  // Stats test requires examining the upgrade itself.
  val shouldUpgrade = new AtomicBoolean(true)

  /**
   * The client and server start with the plain-text upgrade so the first request
   * is actually a HTTP/1.x request and all subsequent requests are legit HTTP/2, so, we
   * fire a throw-away request first so we are testing a real HTTP/2 connection.
   */
  override def initClient(client: HttpService): Unit = {
    if (shouldUpgrade.get()) {
      val request = Request("/")
      await(client(request))
      statsRecv.clear()
    }
  }

  override def initService: HttpService = Service.mk { req: Request =>
    Future.value(Response())
  }

  def featureImplemented(feature: Feature): Boolean = feature != MaxHeaderSize

  test("Upgrade stats are properly recorded") {
    shouldUpgrade.set(false)

    val client = nonStreamingConnect(Service.mk { req: Request =>
      Future.value(Response())
    })

    await(client(Request("/"))) // Should be an upgrade request

    assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 1)
    assert(statsRecv.counters(Seq("server", "upgrade", "success")) == 1)
    await(client.close())

    shouldUpgrade.set(true)
  }

  // TODO: Consolidate behavior between h1 and h2
  test("Client sets & enforces MaxHeaderSize") {
    val server = serverImpl()
      .serve("localhost:*", initService)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withMaxHeaderSize(1.kilobyte)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    initClient(client)

    val req = Request("/")
    req.headerMap.set("foo", "*" * 2.kilobytes.bytes.toInt)
    assert(await(client(req)).status == Status.RequestHeaderFieldsTooLarge)

    await(client.close())
    await(server.close())
  }

  test("H1 related connection headers are stripped") {
    val connectionHeaders = Seq(
      "Keep-Alive",
      "Proxy-Connection",
      "Upgrade",
      "Transfer-Encoding",
      "TE",
      "custom1",
      "custom2"
    )
    val client = nonStreamingConnect(Service.mk { req: Request =>
      val res = Response()
      connectionHeaders.foreach(res.headerMap.add(_, "bad"))
      res.headerMap.add("Connection", "custom1")
      res.headerMap.add("Connection", "custom2")
      res.headerMap.add("ok-header", ":)")
      Future.value(res)
    })

    val rh = await(client(Request("/"))).headerMap
    connectionHeaders.foreach { header =>
      assert(rh.get(header).isEmpty)
    }
    assert(rh.get("ok-header").get == ":)")
  }

  test("The TE header is allowed iff its value is trailers") {
    val client = nonStreamingConnect(Service.mk { req: Request =>
      val res = Response()
      res.headerMap.add("TE", "trailers")
      Future.value(res)
    })

    val rh = await(client(Request("/"))).headerMap
    assert(rh.get("TE").get == "trailers")
  }
}
