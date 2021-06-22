package com.twitter.finagle.http

import com.twitter.finagle.{Address, Failure, FailureFlags, Name, Service, http, Http => FHttp}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Closable, Duration, Future, Throw}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.funsuite.AnyFunSuite

class NackTest extends AnyFunSuite {
  class ClientCtx {
    val n = new AtomicInteger()
    var response = Failure.rejected("unhappy")
    val flakyService = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        if (n.get == -2) {
          Future.exception(new Exception)
        } else if (n.get == -1) {
          Future.exception(
            response.unflagged(FailureFlags.Retryable).flagged(FailureFlags.NonRetryable)
          )
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
        FHttp.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        FHttp.client
          .configured(Stats(clientSr))
          .newService(
            Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
            "http"
          )

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
        FHttp.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        FHttp.client
          .configured(Stats(clientSr))
          .newService(
            Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
            "http"
          )

      n.set(-1)
      Await.result(client(request).liftToTry, timeout) match {
        case Throw(f: Failure) =>
          assert(f.isFlagged(FailureFlags.Rejected) && f.isFlagged(FailureFlags.NonRetryable))
        case _ => fail("Response was not a non-restartable failure")
      }

      assert(!clientSr.counters.contains(Seq("http-client", "requeue", "requeues")))
      assert(serverSr.counters(Seq("myservice", "nonretryable_nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("HttpNack works with ClientBuilder") {
    new ClientCtx {
      val server =
        FHttp.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        ClientBuilder()
          .name("http")
          .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
          .stack(FHttp.client)
          .reportTo(clientSr)
          .hostConnectionLimit(1)
          .build()

      assert(Await.result(client(request), timeout).status == Status.Ok)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 1)

      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("a server that doesn't support HttpNack fails the request") {
    new ClientCtx {
      val server =
        FHttp.server
          .withStack(StackServer.newStack[http.Request, http.Response])
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)

      val client =
        FHttp.client
          .configured(Stats(clientSr))
          .newService(
            Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
            "http-client"
          )

      val rep = Await.result(client(request), timeout)
      assert(rep.status == Status.InternalServerError)
      assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == None)
      assert(!clientSr.counters.contains(Seq("http-client", "requeue", "requeues")))

      n.set(-1)
      val rep2 = Await.result(client(request), timeout)
      assert(rep2.status == Status.InternalServerError)
      assert(rep2.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)

      assert(serverSr.counters(Seq("myservice", "success")) == 0)
      assert(serverSr.counters(Seq("myservice", "failures")) == 2)
      assert(!serverSr.counters.contains(Seq("myservice", "nacks")))
      assert(!serverSr.counters.contains(Seq("myservice", "nonretryable_nacks")))
      Closable.all(client, server).close()
    }
  }

  test("HttpNack does not convert non-rejected, non-restartable failures") {
    new ClientCtx {
      n.set(-2)
      val server =
        FHttp.server
          .configured(Stats(serverSr))
          .configured(Label("myservice"))
          .serve(new InetSocketAddress(0), flakyService)
      val client =
        FHttp.client
          .configured(Stats(clientSr))
          .newService(
            Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
            "http-client"
          )

      val rep = Await.result(client(request), timeout)
      assert(rep.status == Status.InternalServerError)
      assert(rep.headerMap.get(HttpNackFilter.RetryableNackHeader) == None)
      assert(rep.headerMap.get(HttpNackFilter.NonRetryableNackHeader) == None)
      assert(!clientSr.counters.contains(Seq("http-client", "requeue", "requeues")))

      assert(serverSr.counters(Seq("myservice", "success")) == 0)
      assert(serverSr.counters(Seq("myservice", "failures")) == 1)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 0)
      assert(serverSr.counters(Seq("myservice", "nonretryable_nacks")) == 0)

      Closable.all(client, server).close()
    }
  }
}
