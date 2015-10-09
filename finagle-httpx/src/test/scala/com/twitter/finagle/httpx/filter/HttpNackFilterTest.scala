package com.twitter.finagle.httpx.filter

import com.twitter.finagle.{ChannelClosedException, Failure, Httpx, Name, Service}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.httpx.{Request, Response, Status, Http}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpNackFilterTest extends FunSuite {
  class ClientCtx {
    val n = new AtomicInteger()
    val flakyService = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        if (n.get < 0) Future.exception(new Exception)
        else if (n.getAndIncrement == 0) Future.exception(Failure.rejected("unhappy"))
        else Future.value(Response(Status.Ok))
      }
    }
    val request = Request("/")
    val st = new InMemoryStatsReceiver
  }

  test("automatically retries with HttpNack") {
    new ClientCtx {
      val server = Httpx.server.serve(new InetSocketAddress(0), flakyService)
      val client =
        Httpx.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "httpx")

      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("httpx", "requeue", "requeues")) == 1)

      // reuse connections
      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("httpx", "connects")) == 1)
    }
  }

  test("HttpNack works with ClientBuilder") {
    new ClientCtx {
      val server = Httpx.server.serve(new InetSocketAddress(0), flakyService)
      val client =
        ClientBuilder()
          .name("httpx")
          .hosts(server.boundAddress)
          .codec(Http())
          .reportTo(st)
          .hostConnectionLimit(1).build()

      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("httpx", "requeue", "requeues")) == 1)
    }
  }

  test("HttpNack works with ServerBuilder") {
    new ClientCtx {
      val server =
        ServerBuilder()
          .codec(Http())
          .bindTo(new InetSocketAddress(0))
          .name("myservice")
          .build(flakyService)
      val client =
        Httpx.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "httpx")

      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("httpx", "requeue", "requeues")) == 1)
    }
  }

  test("a server that doesn't support HttpNack fails the request") {
    new ClientCtx {
      val server =
        Httpx.server
          .withStack(StackServer.newStack)
          .serve(new InetSocketAddress(0), flakyService)

      val client =
        Httpx.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "httpx")

      intercept[ChannelClosedException](Await.result(client(request)))
    }
  }

  test("HttpNack does not convert non-retryable failures") {
    new ClientCtx {
      n.set(-1)
      val server = Httpx.server.serve(new InetSocketAddress(0), flakyService)
      val client =
        Httpx.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "httpx")

      intercept[ChannelClosedException](Await.result(client(request)))
    }
  }
}