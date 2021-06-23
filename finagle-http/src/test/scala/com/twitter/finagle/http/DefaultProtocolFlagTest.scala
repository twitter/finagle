package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.Http
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Awaitable, Duration, Future}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class DefaultProtocolFlagTest extends AnyFunSuite {

  private[this] def await[T](awaitable: Awaitable[T], timeout: Duration = 5.seconds): T =
    Await.result(awaitable, timeout)

  private case class TestResources(
    svc: Service[Request, Response],
    stats: InMemoryStatsReceiver)

  private[this] def testWithResources[T](
    srv: Http.Server,
    clnt: Http.Client
  )(
    f: TestResources => T
  ): T = {
    val stats = new InMemoryStatsReceiver
    val server =
      srv
        .withStatsReceiver(stats.scope("server"))
        .serve(":0", Service.const(Future(Response())))

    try {
      val client = clnt
        .withStatsReceiver(stats)
        .newService(
          "localhost:" + server.boundAddress.asInstanceOf[InetSocketAddress].getPort,
          "client"
        )
      try {
        f(TestResources(client, stats))
      } finally {
        await(client.close(2.seconds))
      }
    } finally {
      await(server.close(2.seconds))
    }
  }

  test("defaultClientProtocol#Default is used when not set") {
    defaultClientProtocol.letClear {
      testWithResources(Http.server, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 1)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 1)
      }
    }
  }

  test("defaultClientProtocol#Explicit configuration takes precedence when set") {
    defaultClientProtocol.letParse("HTTP/1.1") {
      testWithResources(Http.server, Http.client.withHttp2) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 1)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 1)
      }
    }
    defaultClientProtocol.letParse("HTTP/2") {
      testWithResources(Http.server, Http.client.withNoHttp2) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 0)
          assert(stats.counters.get(Seq("client", "upgrade", "success")) == None)
      }
    }
  }

  test("defaultClientProtocol#Override takes precedence when no explicit configuration set") {
    defaultClientProtocol.letParse("HTTP/1.1") {
      testWithResources(Http.server, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 0)
          assert(stats.counters.get(Seq("client", "upgrade", "success")) == None)
      }
    }
    defaultClientProtocol.letParse("HTTP/2") {
      testWithResources(Http.server, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 1)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 1)
      }
    }
  }

  test("defaultClientProtocol#Unsupported protocol") {
    intercept[IllegalArgumentException] {
      defaultClientProtocol.letParse("HTTP/1.0") {}
    }
    val e = intercept[IllegalArgumentException] {
      defaultClientProtocol.letParse("HTTP/3") {}
    }

    assert(
      e.getMessage == "'HTTP/3' is not a supported Finagle HTTP Protocol. " +
        "Must be one of {'HTTP/1.1', 'HTTP/2'}.")
  }

  test("defaultServerProtocol#Default is used when not set") {
    defaultServerProtocol.letClear {
      testWithResources(Http.server, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 1)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 1)
      }
    }
  }

  test("defaultServerProtocol#Explicit configuration takes precedence when set") {
    defaultServerProtocol.letParse("HTTP/1.1") {
      testWithResources(Http.server.withHttp2, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 1)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 1)
      }
    }
    defaultServerProtocol.letParse("HTTP/2") {
      testWithResources(Http.server.withNoHttp2, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters.get(Seq("server", "upgrade", "success")) == None)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 0)
      }
    }
  }

  test("defaultServerProtocol#Override takes precedence when no explicit configuration set") {
    defaultServerProtocol.letParse("HTTP/1.1") {
      testWithResources(Http.server, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters.get(Seq("server", "upgrade", "success")) == None)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 0)
      }
    }

    defaultServerProtocol.letParse("HTTP/2") {
      testWithResources(Http.server, Http.client) {
        case TestResources(clnt, stats) =>
          Await.result(clnt(Request("/")))
          assert(stats.counters(Seq("server", "upgrade", "success")) == 1)
          assert(stats.counters(Seq("client", "upgrade", "success")) == 1)
      }
    }
  }

  test("defaultServerProtocol#Unsupported protocol") {
    intercept[IllegalArgumentException] {
      defaultServerProtocol.letParse("HTTP/1.0") {}
    }
    val e = intercept[IllegalArgumentException] {
      defaultServerProtocol.letParse("HTTP/3") {}
    }

    assert(
      e.getMessage == "'HTTP/3' is not a supported Finagle HTTP Protocol. " +
        "Must be one of {'HTTP/1.1', 'HTTP/2'}.")
  }

}
