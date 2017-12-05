package com.twitter.finagle.http

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.Service
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.toggle.flag.overrides
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

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
  // note that this behavior is implementation-dependent
  // the spec says MaxHeaderListSize is advisory
  test("Server sets & enforces MaxHeaderSize") {
    val server = serverImpl()
      .withMaxHeaderSize(1.kilobyte)
      .serve("localhost:*", initService)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
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

  test("The upgrade request is ineligible for flow control") {
    val server = serverImpl()
      .withMaxHeaderSize(1.kilobyte)
      .serve("localhost:*", Service.mk[Request, Response] { _ =>
        // we need to make this slow or else it'll race the window updating
        Future.sleep(50.milliseconds)(DefaultTimer).map(_ => Response())
      })

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val request = Request(Method.Post, "/")
    // send a request that the client *should* have fragmented if it was
    // sending an http/2 message
    request.content = Buf.Utf8("*" * 70000)

    // check that this doesn't throw an exception
    val rep = await(client(request))
    assert(rep.status == Status.Ok)
  }

  test("Upgrades to HTTP/2 only if both have the toggle on, and it's H2C, not H2") {
    for {
      clientUseHttp2 <- Seq(1D, 0D)
      serverUseHttp2 <- Seq(1D, 0D)
      toggleName <- Seq("com.twitter.finagle.http.UseH2", "com.twitter.finagle.http.UseH2C")
    } {
      val sr = new InMemoryStatsReceiver()
      val server = overrides.let(Map(toggleName -> serverUseHttp2)) {
        finagle.Http.server
          .withStatsReceiver(sr)
          .withLabel("server")
          .serve("localhost:*", initService)
      }
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = overrides.let(Map(toggleName -> clientUseHttp2)) {
        finagle.Http.client
          .withStatsReceiver(sr)
          .newService(s"${addr.getHostName}:${addr.getPort}", "client")
      }
      val rep = client(Request("/"))
      await(rep)
      if (
        clientUseHttp2 == 1.0 &&
          serverUseHttp2 == 1.0 &&
          toggleName == "com.twitter.finagle.http.UseH2C"
      ) {
        assert(sr.counters.get(Seq("client", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on")
        assert(sr.counters.get(Seq("server", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on")
      } else {
        val clientStatus = if (clientUseHttp2 == 1) "on" else "off"
        val serverStatus = if (serverUseHttp2 == 1) "on" else "off"
        val errorMsg = s"Upgraded when the client was $clientStatus, the server was " +
          s"$serverStatus, the toggle was $toggleName"
        assert(!sr.counters.contains(Seq("client", "upgrade", "success")), errorMsg)
        assert(!sr.counters.contains(Seq("server", "upgrade", "success")), errorMsg)
      }
      await(Closable.all(client, server).close())
    }
  }
}
