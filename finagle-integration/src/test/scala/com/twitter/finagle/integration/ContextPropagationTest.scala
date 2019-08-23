package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.Retries
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.thrift.{Protocols, ThriftUtil}
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Return}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class ContextPropagationTest extends FunSuite with MockitoSugar {

  case class TestContext(buf: Buf)

  val testContext = new Contexts.broadcast.Key[TestContext]("com.twitter.finagle.mux.MuxContext") {
    def marshal(tc: TestContext) = tc.buf
    def tryUnmarshal(buf: Buf) = Return(TestContext(buf))
  }

  trait ThriftMuxTestServer {
    val server = ThriftMux.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.FutureIface {
        def query(x: String): Future[String] =
          (Contexts.broadcast.get(testContext), Dtab.local) match {
            case (None, Dtab.empty) =>
              Future.value(x + x)

            case (Some(TestContext(buf)), _) =>
              val Buf.Utf8(str) = buf
              Future.value(str)

            case (_, dtab) =>
              Future.value(dtab.show)
          }

        def question(y: String): Future[String] =
          (Contexts.broadcast.get(testContext), Dtab.local) match {
            case (None, Dtab.empty) =>
              Future.value(y + y)

            case (Some(TestContext(buf)), _) =>
              val Buf.Utf8(str) = buf
              Future.value(str)

            case (_, dtab) =>
              Future.value(dtab.show)
          }

        def inquiry(z: String): Future[String] =
          (Contexts.broadcast.get(testContext), Dtab.local) match {
            case (None, Dtab.empty) =>
              Future.value(z + z)

            case (Some(TestContext(buf)), _) =>
              val Buf.Utf8(str) = buf
              Future.value(str)

            case (_, dtab) =>
              Future.value(dtab.show)
          }
      }
    )
  }

  test("thriftmux server + thriftmux client: propagate Contexts") {
    new ThriftMuxTestServer {
      val client = ThriftMux.client.newIface[TestService.FutureIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

      assert(Await.result(client.query("ok"), 5.second) == "okok")

      Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("hello context world"))) {
        assert(Await.result(client.query("ok"), 5.second) == "hello context world")
      }

      Await.result(server.close(), 5.second)
    }
  }

  test("thriftmux server + Finagle thrift client: propagate Contexts") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.newIface[TestService.FutureIface](
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "client"
        )

      assert(Await.result(client.query("ok"), 5.second) == "okok")

      Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("hello context world"))) {
        assert(Await.result(client.query("ok"), 5.second) == "hello context world")
      }

      Await.result(server.close(), 5.second)
    }
  }

  test("thriftmux server + Finagle thrift client: propagate Dtab.local") {
    new ThriftMuxTestServer {
      val client =
        Thrift.client.newIface[TestService.FutureIface](
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "client"
        )

      assert(Await.result(client.query("ok"), 5.second) == "okok")

      Dtab.unwind {
        Dtab.local = Dtab.read("/foo=>/bar")
        assert(Await.result(client.query("ok"), 5.second) == "/foo=>/bar")
      }

      Await.result(server.close(), 5.second)
    }
  }

  test("thriftmux server + thriftmux client: server sees Retries set by client") {
    val iface = new TestService.FutureIface {
      def query(x: String) = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }

    val service = ThriftUtil.serverFromIface(
      iface,
      Protocols.binaryFactory(),
      NullStatsReceiver,
      Int.MaxValue,
      "server"
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
      .newIface[TestService.FutureIface](
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

    val ifaceB = new TestService.FutureIface {
      def query(x: String) = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }

    val serviceB = ThriftUtil.serverFromIface(
      ifaceB,
      Protocols.binaryFactory(),
      NullStatsReceiver,
      Int.MaxValue,
      "serverB"
    )

    val serverB = ThriftMux.server
      .serve(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        assertRetriesFilter.andThen(serviceB)
      )

    val clientB = Thrift.client
      .withStack(_.remove(Retries.Role))
      .newIface[TestService.FutureIface](
        Name.bound(Address(serverB.boundAddress.asInstanceOf[InetSocketAddress])),
        "clientB"
      )

    val ifaceA = new TestService.FutureIface {
      def query(x: String) = clientB.query(x)
      def question(y: String): Future[String] = clientB.question(y)
      def inquiry(z: String): Future[String] = clientB.inquiry(z)
    }

    val serviceA = ThriftUtil.serverFromIface(
      ifaceA,
      Protocols.binaryFactory(),
      NullStatsReceiver,
      Int.MaxValue,
      "serverA"
    )

    val serverA = ThriftMux.server
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), serviceA)

    val clientA = Thrift.client
      .newIface[TestService.FutureIface](
        Name.bound(Address(serverA.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    assert(Await.result(clientA.query("ok")) == "ok")
  }
}
