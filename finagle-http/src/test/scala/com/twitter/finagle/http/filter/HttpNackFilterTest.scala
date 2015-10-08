package com.twitter.finagle.http.filter

import com.twitter.finagle.{ChannelClosedException, Failure, Http, Name, Service}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http.{Request, Response, Status}
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
      val server = Http.server.serve(new InetSocketAddress(0), flakyService)
      val client =
        Http.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "http")

      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("http", "requeue", "requeues")) == 1)

      // reuse connections
      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("http", "connects")) == 1)
    }
  }

  test("HttpNack works with ClientBuilder") {
    new ClientCtx {
      val server = Http.server.serve(new InetSocketAddress(0), flakyService)
      val client =
        ClientBuilder()
          .name("http")
          .hosts(server.boundAddress)
          .codec(com.twitter.finagle.http.Http())
          .reportTo(st)
          .hostConnectionLimit(1).build()

      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("http", "requeue", "requeues")) == 1)
    }
  }

  test("HttpNack works with ServerBuilder") {
    new ClientCtx {
      val server =
        ServerBuilder()
          .codec(com.twitter.finagle.http.Http())
          .bindTo(new InetSocketAddress(0))
          .name("myservice")
          .build(flakyService)
      val client =
        Http.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "http")

      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("http", "requeue", "requeues")) == 1)
    }
  }

  test("a server that doesn't support HttpNack fails the request") {
    new ClientCtx {
      val server =
        Http.server
          .withStack(StackServer.newStack)
          .serve(new InetSocketAddress(0), flakyService)

      val client =
        Http.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "http")

      intercept[ChannelClosedException](Await.result(client(request)))
    }
  }

  test("HttpNack does not convert non-retryable failures") {
    new ClientCtx {
      n.set(-1)
      val server = Http.server.serve(new InetSocketAddress(0), flakyService)
      val client =
        Http.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "http")

      intercept[ChannelClosedException](Await.result(client(request)))
    }
  }
}
