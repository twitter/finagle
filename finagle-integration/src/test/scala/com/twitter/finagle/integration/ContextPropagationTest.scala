package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.service.Retries
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.thrift.{Protocols, RichServerParam, ThriftUtil}
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Return}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ContextPropagationTest extends AnyFunSuite with MockitoSugar {

  case class TestContext(buf: Buf)

  val testContext = new Contexts.broadcast.Key[TestContext]("com.twitter.finagle.mux.MuxContext") {
    def marshal(tc: TestContext) = tc.buf
    def tryUnmarshal(buf: Buf) = Return(TestContext(buf))
  }

  trait ThriftMuxTestServer {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.MethodPerEndpoint {
        def query(x: String): Future[String] =
          (Contexts.broadcast.get(testContext), Dtab.local, Dtab.limited) match {
            case (_, _, dtab) if dtab != Dtab.empty =>
              Future.exception(new IllegalStateException(s"Dtab.limited propagated: ${dtab.show}"))
            case (None, Dtab.empty, _) =>
              Future.value(x + x)

            case (Some(TestContext(buf)), _, _) =>
              val Buf.Utf8(str) = buf
              Future.value(str)

            case (_, dtab, _) =>
              Future.value(dtab.show)
          }

        def question(y: String): Future[String] =
          (Contexts.broadcast.get(testContext), Dtab.local, Dtab.limited) match {
            case (_, _, dtab) if dtab != Dtab.empty =>
              Future.exception(new IllegalStateException(s"Dtab.limited propagated: ${dtab.show}"))
            case (None, Dtab.empty, _) =>
              Future.value(y + y)

            case (Some(TestContext(buf)), _, _) =>
              val Buf.Utf8(str) = buf
              Future.value(str)

            case (_, dtab, _) =>
              Future.value(dtab.show)
          }

        def inquiry(z: String): Future[String] =
          (Contexts.broadcast.get(testContext), Dtab.local, Dtab.limited) match {
            case (_, _, dtab) if dtab != Dtab.empty =>
              Future.exception(new IllegalStateException(s"Dtab.limited propagated: ${dtab.show}"))
            case (None, Dtab.empty, _) =>
              Future.value(z + z)

            case (Some(TestContext(buf)), _, _) =>
              val Buf.Utf8(str) = buf
              Future.value(str)

            case (_, dtab, _) =>
              Future.value(dtab.show)
          }
      }
    )
  }

  test("thriftmux server + thriftmux client: propagate Contexts") {
    new ThriftMuxTestServer {
      val client = ThriftMux.client.build[TestService.MethodPerEndpoint](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

      assert(Await.result(client.query("ok"), 5.seconds) == "okok")

      Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("hello context world"))) {
        assert(Await.result(client.query("ok"), 5.seconds) == "hello context world")
      }

      Await.result(server.close(), 5.seconds)
    }
  }

  test("thriftmux server + Finagle thrift client: propagate Contexts") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.build[TestService.MethodPerEndpoint](
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "client"
        )

      assert(Await.result(client.query("ok"), 5.seconds) == "okok")

      Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("hello context world"))) {
        assert(Await.result(client.query("ok"), 5.seconds) == "hello context world")
      }

      Await.result(server.close(), 5.seconds)
    }
  }

  test("thriftmux server + Finagle thrift client: propagate Dtab.local") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.build[TestService.MethodPerEndpoint](
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "client"
        )

      assert(Await.result(client.query("ok"), 5.seconds) == "okok")

      Dtab.unwind {
        Dtab.local = Dtab.read("/foo=>/bar")
        assert(Await.result(client.query("ok"), 5.seconds) == "/foo=>/bar")
      }

      Await.result(server.close(), 5.seconds)
    }
  }

  val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
  def mkService(): Service[Request, Response] = {
    new Service[Request, Response] {
      override def apply(request: Request): Future[Response] = {
        val response = Response(Status.Ok)
        val localSize = Dtab.local.dentries0.length.toString
        val limitedSize = Dtab.limited.dentries0.length.toString
        response.contentString = s"local:${localSize} limited:${limitedSize}"
        Future.value(response)
      }
    }
  }
  test("Http server + Http client:  propagate Dtab.local") {
    val service = mkService()
    val http = Http.server
      .withLabel("someservice")
      .serve(address, service)

    val httpAddr = http.boundAddress.asInstanceOf[InetSocketAddress]
    val client = Http.client.newService(s":${httpAddr.getPort}", "http")
    val request = Request("/foo")
    val response = Await.result(client(request), 5.seconds)

    assert(response.contentString == "local:0 limited:0")

    Dtab.unwind {
      Dtab.local = Dtab.read("/foo=>/bar")
      assert(Await.result(client(request), 5.seconds).contentString == "local:1 limited:0")
    }

    Await.result(client.close(), 5.seconds)
  }

  test("Http server + Http client: do NOT propagate Dtab.limited") {
    val service = mkService()
    val http = Http.server
      .withLabel("someservice")
      .serve(address, service)

    val httpAddr = http.boundAddress.asInstanceOf[InetSocketAddress]
    val client = Http.client.newService(s":${httpAddr.getPort}", "http")
    val request = Request("/foo")
    val response = Await.result(client(request), 5.seconds)

    assert(response.contentString == "local:0 limited:0")

    Dtab.unwind {
      Dtab.limited = Dtab.read("/foo=>/bar")
      assert(Await.result(client(request), 5.seconds).contentString == "local:0 limited:0")
    }

    Await.result(client.close(), 5.seconds)
  }

  test("thriftmux server + Finagle thrift client: do NOT propagate Dtab.limited") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.build[TestService.MethodPerEndpoint](
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "client"
        )

      assert(Await.result(client.query("ok"), 5.seconds) == "okok")

      Dtab.unwind {
        Dtab.limited = Dtab.read("/foo=>/bar")
        assert(Await.result(client.query("ok"), 5.seconds) == "okok")
      }

      Await.result(server.close(), 5.seconds)
    }
  }

  test("thriftmux server + thriftmux client: server sees Retries set by client") {
    val iface = new TestService.MethodPerEndpoint {
      def query(x: String) = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }

    val service = ThriftUtil.serverFromIface(
      iface,
      RichServerParam(
        Protocols.binaryFactory(),
        "server",
        Int.MaxValue,
        NullStatsReceiver
      )
    )

    val assertRetriesFilter = new SimpleFilter[Array[Byte], Array[Byte]] {
      def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) = {
        assert(context.Retries.current == Some(context.Retries(0)))
        service(request)
      }
    }

    val server = ThriftMux.server.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      assertRetriesFilter.andThen(service)
    )

    val client = Thrift.client
      .build[TestService.MethodPerEndpoint](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    assert(Await.result(client.query("ok"), 5.seconds) == "ok")
  }

  test(
    "thriftmux server + thriftmux client: server does not see Retries " +
      "set by another client if client removed RequeueFilter"
  ) {
    val assertRetriesFilter = new SimpleFilter[Array[Byte], Array[Byte]] {
      def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]) = {
        assert(context.Retries.current == None)
        service(request)
      }
    }

    // clientA -> ServerA:clientB -> ServerB
    // client B has had its Retry module removed.

    val ifaceB = new TestService.MethodPerEndpoint {
      def query(x: String) = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }

    val serviceB = ThriftUtil.serverFromIface(
      ifaceB,
      RichServerParam(
        Protocols.binaryFactory(),
        "serverB",
        Int.MaxValue,
        NullStatsReceiver
      )
    )

    val serverB = ThriftMux.server
      .serve(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        assertRetriesFilter.andThen(serviceB)
      )

    val clientB = Thrift.client
      .withStack(_.remove(Retries.Role))
      .build[TestService.MethodPerEndpoint](
        Name.bound(Address(serverB.boundAddress.asInstanceOf[InetSocketAddress])),
        "clientB"
      )

    val ifaceA = new TestService.MethodPerEndpoint {
      def query(x: String) = clientB.query(x)
      def question(y: String): Future[String] = clientB.question(y)
      def inquiry(z: String): Future[String] = clientB.inquiry(z)
    }

    val serviceA = ThriftUtil.serverFromIface(
      ifaceA,
      RichServerParam(
        Protocols.binaryFactory(),
        "serverA",
        Int.MaxValue,
        NullStatsReceiver
      )
    )

    val serverA = ThriftMux.server
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), serviceA)

    val clientA = Thrift.client
      .build[TestService.MethodPerEndpoint](
        Name.bound(Address(serverA.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    assert(Await.result(clientA.query("ok")) == "ok")
  }
}
