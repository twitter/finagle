package com.twitter.finagle.http.filter

import com.twitter.finagle.{Address, Failure, Http, Name, Service}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util.{Await, Closable, Duration, Future, Throw}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpNackFilterTest extends FunSuite {
  class ClientCtx {
    val n = new AtomicInteger()
    var response = Failure.rejected("unhappy")
    val flakyService = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        if (n.get == -2) {
          Future.exception(new Exception)
        } else if (n.get == -1) {
          Future.exception(response.unflagged(Failure.Restartable).flagged(Failure.NonRetryable))
        } else if (n.getAndIncrement == 0) {
          Future.exception(response)
        } else {
          Future.value(Response(Status.Ok))
        }
      }
    }
    val request = Request("/")
    val serverSr = new InMemoryStatsReceiver
    val clientSr = new InMemoryStatsReceiver
  }

  val timeout = Duration.fromSeconds(30)

  test("automatically retries with HttpNack if restartable") {
    new ClientCtx {
      val server =
        Http.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        Http.client
          .configured(Stats(clientSr))
          .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "http")

      assert(Await.result(client(request), timeout).status == Status.Ok)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 1)

      // reuse connections
      assert(Await.result(client(request), timeout).status == Status.Ok)
      assert(clientSr.counters(Seq("http", "connects")) == 1)

      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("converts non-restartable/non-retryable Failures") {
    new ClientCtx {
      val server =
        Http.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        Http.client
          .configured(Stats(clientSr))
          .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "http")

      n.set(-1)
      Await.result(client(request).liftToTry, timeout) match {
        case Throw(f: Failure) => assert(f.isFlagged(Failure.Rejected) && f.isFlagged(Failure.NonRetryable))
        case _ => fail("Response was not a non-restartable failure")
      }

      assert(clientSr.counters.get(Seq("http-client", "requeue", "requeues")) == None)
      assert(serverSr.counters(Seq("myservice", "nonretryable_nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("HttpNack works with ClientBuilder") {
    new ClientCtx {
      val server =
        Http.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        ClientBuilder()
          .name("http")
          .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
          .codec(com.twitter.finagle.http.Http())
          .reportTo(clientSr)
          .hostConnectionLimit(1).build()

      assert(Await.result(client(request), timeout).status == Status.Ok)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 1)

      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("HttpNack works with ServerBuilder") {
    new ClientCtx {
      val serverLabel = "myservice"
      val server =
        ServerBuilder()
          .codec(http.Http(_statsReceiver = serverSr.scope(serverLabel)))
          .bindTo(new InetSocketAddress(0))
          .name(serverLabel)
          .reportTo(serverSr)
          .build(flakyService)
      val client =
        Http.client
          .configured(Stats(clientSr))
          .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "http")

      assert(Await.result(client(request), timeout).status == Status.Ok)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 1)

      n.set(-1)
      assert(Await.result(client(request).liftToTry, timeout).isThrow)

      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)
      assert(serverSr.counters(Seq("myservice", "nonretryable_nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("a server that doesn't support HttpNack fails the request") {
    new ClientCtx {
      val server =
        Http.server
          .withStack(StackServer.newStack)
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)

      val client =
        Http.client
          .configured(Stats(clientSr))
          .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "http-client")

      val rep = Await.result(client(request), timeout)
      assert(rep.status == Status.InternalServerError)
      assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == None)
      assert(clientSr.counters.get(Seq("http-client", "requeue", "requeues")) == None)

      n.set(-1)
      val rep2 = Await.result(client(request), timeout)
      assert(rep2.status == Status.InternalServerError)
      assert(rep2.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)

      assert(serverSr.counters.get(Seq("myservice", "success")) == None)
      assert(serverSr.counters(Seq("myservice", "failures")) == 2)
      assert(serverSr.counters.get(Seq("myservice", "nacks")) == None)
      assert(serverSr.counters.get(Seq("myservice", "nonretryable_nacks")) == None)
      Closable.all(client, server).close()
    }
  }

  test("HttpNack does not convert non-rejected, non-restartable failures") {
    new ClientCtx {
      n.set(-2)
      val server =
        Http.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        Http.client
          .configured(Stats(clientSr))
          .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "http-client")

      val rep = Await.result(client(request), timeout)
      assert(rep.status == Status.InternalServerError)
      assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == None)
      assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
      assert(clientSr.counters.get(Seq("http-client", "requeue", "requeues")) == None)

      assert(serverSr.counters.get(Seq("myservice", "success")) == None)
      assert(serverSr.counters(Seq("myservice", "failures")) == 1)
      assert(serverSr.counters.get(Seq("myservice", "nacks")) == None)
      assert(serverSr.counters.get(Seq("myservice", "nonretryable_nacks")) == None)

      Closable.all(client, server).close()
    }
  }

  test("HttpNackFilter returns a body when rejecting a request that isnt a HEAD request") {
    val stats = new NullStatsReceiver
    val service = new HttpNackFilter(stats) andThen Service.mk { _: Request => Future.exception(Failure.rejected("sadface")) }

    val canHaveBodies = Set(
      Method.Get,
      Method.Post,
      Method.Put,
      Method.Patch,
      Method.Delete,
      Method.Trace,
      Method.Connect,
      Method.Options
    )

    canHaveBodies.foreach { method =>
      val request = Request()
      request.method = method

      val rep = Await.result(service(request), timeout)
      assert(rep.status == Status.ServiceUnavailable)
      assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == Some("true"))
      assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
      assert(!rep.content.isEmpty)
    }
  }

  test("HttpNackFilter does not return a body when rejecting a HEAD request") {
    val stats = new NullStatsReceiver
    val service = new HttpNackFilter(stats) andThen Service.mk { _: Request => Future.exception(Failure.rejected("sadface")) }

    val request = Request()
    request.method = Method.Head

    val rep = Await.result(service(request), timeout)
    assert(rep.status == Status.ServiceUnavailable)
    assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == Some("true"))
    assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
    assert(rep.content.isEmpty)
  }
}
