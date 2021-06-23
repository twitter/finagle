package com.twitter.finagle.http

import com.twitter.conversions.StorageUnitOps._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http2.RstException
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.service.ServiceFactoryRef
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.util._
import io.netty.handler.codec.http2.Http2CodecUtil
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

abstract class AbstractH2CEndToEndTest extends AbstractHttp2EndToEndTest {

  def clientImpl(): finagle.Http.Client =
    finagle.Http.client.withHttp2
      .withStatsReceiver(statsRecv)

  def serverImpl(): finagle.Http.Server = finagle.Http.server.withHttp2

  // Stats test requires examining the upgrade itself.
  private[this] val ShouldUpgrade = Contexts.local.newKey[Boolean]()

  /**
   * The client and server start with the plain-text upgrade so the first request
   * is actually a HTTP/1.x request and all subsequent requests are legit HTTP/2, so, we
   * fire a throw-away request first so we are testing a real HTTP/2 connection.
   */
  override def initClient(client: HttpService): Unit = {
    if (Contexts.local.get(ShouldUpgrade).getOrElse(true)) {
      val request = Request("/")
      await(client(request))
      eventually {
        assert(statsRecv.counters(Seq("client", "requests")) == 1L)
      }
      statsRecv.clear()
    }
  }

  override def initService: HttpService = Service.mk { _: Request => Future.value(Response()) }

  def featureImplemented(feature: Feature): Boolean = true

  test("Upgrade stats are properly recorded") {
    Contexts.local.let(ShouldUpgrade, false) {
      val client = nonStreamingConnect(Service.mk { _: Request => Future.value(Response()) })

      await(client(Request("/"))) // Should be an upgrade request

      assert(statsRecv.counters(Seq("client", "upgrade", "success")) == 1)
      assert(statsRecv.counters(Seq("server", "upgrade", "success")) == 1)
      await(client.close())
    }
  }

  test("Upgrade ignored") {
    val req = Request(Method.Post, "/")
    req.contentString = "body"

    Contexts.local.let(ShouldUpgrade, false) {
      val client = nonStreamingConnect(Service.mk { _: Request => Future.value(Response()) })

      await(client(req))
      // Should have been ignored by upgrade mechanisms since the request has a body
      assert(statsRecv.counters(Seq("client", "upgrade", "ignored")) == 1)

      // Should still be zero since the client didn't attempt the upgrade at all
      assert(statsRecv.counters(Seq("server", "upgrade", "ignored")) == 0)
      await(client.close())
    }

    Contexts.local.let(ShouldUpgrade, false) {
      val client = nonStreamingConnect(Service.mk { _: Request => Future.value(Response()) })

      // Spoof the upgrade: the client won't attempt it but the Upgrade header should
      // still cause the server to consider it an upgrade request and tick the counter.
      req.headerMap.set(Fields.Upgrade, "h2c")

      await(client(req))
      assert(statsRecv.counters(Seq("client", "upgrade", "ignored")) == 2)
      assert(statsRecv.counters(Seq("server", "upgrade", "ignored")) == 1)
      await(client.close())
    }
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
    connectionHeaders.foreach { header => assert(rh.get(header).isEmpty) }
    assert(rh.get("ok-header").get == ":)")
  }

  test("The TE header is allowed if its value is trailers") {
    val client = nonStreamingConnect(Service.mk { _: Request =>
      val res = Response()
      res.headerMap.add("TE", "trailers")
      Future.value(res)
    })

    val rh = await(client(Request("/"))).headerMap
    assert(rh.get("TE").get == "trailers")
  }

  if (!sys.props.contains("SKIP_FLAKY_TRAVIS"))
    test("The upgrade request is ineligible for flow control") {
      val server = serverImpl()
        .withMaxHeaderSize(1.kilobyte)
        .serve(
          "localhost:*",
          Service.mk[Request, Response] { _ =>
            // we need to make this slow or else it'll race the window updating
            Future.sleep(50.milliseconds)(DefaultTimer).map(_ => Response())
          }
        )

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
      clientUseHttp2 <- Seq(true, false)
      serverUseHttp2 <- Seq(true, false)
    } {
      val sr = new InMemoryStatsReceiver()
      val server = {
        val srv = finagle.Http.server
          .withStatsReceiver(sr)
          .withLabel("server")
        (if (serverUseHttp2) srv else srv.withNoHttp2).serve("localhost:*", initService)
      }
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = {
        val clnt = finagle.Http.client
          .withStatsReceiver(sr)
        (if (clientUseHttp2) clnt else clnt.withNoHttp2)
          .newService(s"${addr.getHostName}:${addr.getPort}", "client")
      }
      val rep = client(Request("/"))
      await(rep)
      if (clientUseHttp2 && serverUseHttp2) {
        assert(
          sr.counters.get(Seq("client", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on"
        )
        assert(
          sr.counters.get(Seq("server", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were toggled on"
        )
      } else {
        val clientStatus = if (clientUseHttp2) "on" else "off"
        val serverStatus = if (serverUseHttp2) "on" else "off"
        val errorMsg = s"Upgraded when the client was $clientStatus, the server was " +
          s"$serverStatus"
        val clientSuccess = sr.counters.get(Seq("client", "upgrade", "success"))
        assert(clientSuccess.isEmpty || clientSuccess.contains(0), errorMsg)

        val serverSuccess = sr.counters.get(Seq("server", "upgrade", "success"))
        assert(serverSuccess.isEmpty || serverSuccess.contains(0), errorMsg)
      }
      await(Closable.all(client, server).close())
    }
  }

  test("Configuration params take precedence over the defaults for the client") {
    for {
      clientUseHttp2 <- Seq(true, false)
    } {
      val sr = new InMemoryStatsReceiver()
      val server = serverImpl
        .withStatsReceiver(sr)
        .withLabel("server")
        .serve("localhost:*", initService)
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = {
        val c = finagle.Http.client
          .withStatsReceiver(sr)

        (if (clientUseHttp2) c.withHttp2
         else c.withNoHttp2)
          .newService(s"${addr.getHostName}:${addr.getPort}", "client")
      }
      val rep = client(Request("/"))
      await(rep)
      if (clientUseHttp2) {
        assert(
          sr.counters.get(Seq("client", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were on"
        )
        assert(
          sr.counters.get(Seq("server", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were on"
        )
      } else {
        assert(!sr.counters.contains(Seq("client", "upgrade", "success")))
        val serverSuccess = sr.counters.get(Seq("server", "upgrade", "success"))
        assert(serverSuccess.isEmpty || serverSuccess.contains(0L))
      }
      await(Closable.all(client, server).close())
    }
  }

  test("Configuration params take precedence over the defaults for the server") {
    for {
      serverUseHttp2 <- Seq(true, false)
    } {
      val sr = new InMemoryStatsReceiver()
      val server = {
        val s = finagle.Http.server
          .withStatsReceiver(sr)
          .withLabel("server")

        (if (serverUseHttp2) s.withHttp2
         else s.withNoHttp2)
          .serve("localhost:*", initService)
      }
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = clientImpl()
        .withStatsReceiver(sr)
        .newService(s"${addr.getHostName}:${addr.getPort}", "client")
      val rep = client(Request("/"))
      await(rep)
      if (serverUseHttp2) {
        assert(
          sr.counters.get(Seq("client", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were on"
        )
        assert(
          sr.counters.get(Seq("server", "upgrade", "success")) == Some(1),
          "Failed to upgrade when both parties were on"
        )
      } else {
        assert(sr.counters(Seq("client", "upgrade", "success")) == 0)
        assert(!sr.counters.contains(Seq("server", "upgrade", "success")))
      }
      await(Closable.all(client, server).close())
    }
  }

  test("We delete the HTTP2-SETTINGS header properly") {
    @volatile var headers: HeaderMap = null
    val server = serverImpl().serve(
      "localhost:*",
      Service.mk { req: Request =>
        headers = req.headerMap
        Future.value(Response())
      })
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl().newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    await(client(Request("/")))
    assert(!headers.contains(Http2CodecUtil.HTTP_UPGRADE_SETTINGS_HEADER.toString))
  }

  private final class Srv extends Closable {

    val responses = new ArrayBuffer[Promise[Response]]

    private[this] val ref = new ServiceFactoryRef(ServiceFactory.const(initService))
    private[this] val service = Service.mk[Request, Response] { _ =>
      _pending.incrementAndGet()
      val p = new Promise[Response]
      responses.append(p)
      p.ensure(_pending.decrementAndGet())
    }

    private[this] val _pending = new AtomicInteger

    private[this] val _ls = finagle.Http.server.withHttp2
      .withLabel("server")
      .serve("localhost:*", ref)

    def pending(): Int = _pending.get()
    def startProcessing(idx: Int) = responses(idx).setValue(Response())
    def boundAddr = _ls.boundAddress.asInstanceOf[InetSocketAddress]
    def close(deadline: Time): Future[Unit] = _ls.close(deadline)
    def upgrade(svc: Service[Request, Response]): Unit = {
      initClient(svc)
      ref() = ServiceFactory.const(service)
    }
  }

  test("draining servers process pending requests") {
    val srv = new Srv

    val dest = s"${srv.boundAddr.getHostName}:${srv.boundAddr.getPort}"

    val client =
      finagle.Http.client.withHttp2
        .withStatsReceiver(statsRecv)
        .newService(dest, "client")

    srv.upgrade(client)

    // dispatch a request that will be pending when the
    // server shutsdown.
    val pendingReply = client(Request("/"))
    while (srv.pending() != 1) { Thread.sleep(100) }

    // shutdown server w/ grace period
    srv.close(10.minutes)

    // new connection attempt fails
    val rep2 = client(Request("/"))
    Await.ready(rep2, 30.seconds)
    assert(rep2.poll.get.isThrow)

    srv.startProcessing(0)

    // pending request is finally successfully processed
    Await.ready(pendingReply, 30.seconds)
    pendingReply.poll.get match {
      case Return(resp) => assert(resp.status == Status.Ok)
      case Throw(t) => fail("didn't expect pendingReply to fail", t)
    }
  }

  test("illegal headers produce a non-zero error code on the client") {
    val srv = serverImpl()
      .serve("localhost:*", Service.mk[Request, Response](_ => Future.value(Response())))

    val bound = srv.boundAddress.asInstanceOf[InetSocketAddress]
    val dest = s"${bound.getHostName}:${bound.getPort}"

    val client = clientImpl().newService(dest, "client")

    val req = new Request.Proxy {
      val underlying = Request("/")
      def request: Request = underlying
    }

    initClient(client)

    // this sends illegal pseudo headers to the server, it should reject them with a non-zero
    // error code.
    req.headerMap.setUnsafe(":invalid", "foo")

    val rep = client(req)

    val error = intercept[RstException] {
      Await.result(rep, 5.seconds)
    }
    assert(error.errorCode != 0) // assert that this was not an error-free rejection
  }
}
