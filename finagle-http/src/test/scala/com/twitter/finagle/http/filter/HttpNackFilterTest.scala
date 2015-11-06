package com.twitter.finagle.http.filter

import com.twitter.finagle.{ChannelClosedException, Failure, Http, Name, Service}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Closable, Await, Future}
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
      assert(st.counters(Seq("http", "retries", "requeues")) == 1)

      // reuse connections
      assert(Await.result(client(request)).status == Status.Ok)
      assert(st.counters(Seq("http", "connects")) == 1)

      Closable.all(client, server).close()
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
      assert(st.counters(Seq("http", "retries", "requeues")) == 1)

      Closable.all(client, server).close()
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
      assert(st.counters(Seq("http", "retries", "requeues")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("a server that doesn't support HttpNack fails the request") {
    new ClientCtx {
      val server =
        Http.server
          .withStack(StackServer.newStack)
          .configured(Stats(st))
          .configured(Label("http-server"))
          .serve(new InetSocketAddress(0), flakyService)

      val client =
        Http.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "http-client")

      val rep = Await.result(client(request))
      assert(rep.status == Status.InternalServerError)
      assert(rep.headerMap.get(HttpNackFilter.Header) == None)
      assert(st.counters.get(Seq("http-client", "requeue", "requeues")) == None)
      assert(st.counters.get(Seq("http-server", "success")) == None)
      assert(st.counters(Seq("http-server", "failures")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("HttpNack does not convert non-retryable failures") {
    new ClientCtx {
      n.set(-1)
      val server =
        Http.server
          .configured(Stats(st))
          .configured(Label("http-server"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        Http.client
          .configured(Stats(st))
          .newService(Name.bound(server.boundAddress), "http-client")

      val rep = Await.result(client(request))
      assert(rep.status == Status.InternalServerError)
      assert(rep.headerMap.get(HttpNackFilter.Header) == None)
      assert(st.counters.get(Seq("http-client", "requeue", "requeues")) == None)
      assert(st.counters.get(Seq("http-server", "success")) == None)
      assert(st.counters(Seq("http-server", "failures")) == 1)

      Closable.all(client, server).close()
    }
  }
}
