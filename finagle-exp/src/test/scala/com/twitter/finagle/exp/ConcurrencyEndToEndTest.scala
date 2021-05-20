package com.twitter.finagle.exp

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.exp.ConcurrencyLimitFilter.ConcurrencyOverload
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Duration, Future, Promise, Throw}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite

class ConcurrencyEndToEndTest extends AnyFunSuite with Eventually with IntegrationPatience {
  private[this] val initialConcurrentReqLimit = 10
  private[this] val maxConcurrentReqLimit = 1000
  private[this] val excessRps = 5

  // Timeout needs to be generous so CI doesn't fail
  private[this] val timeout = 10.seconds

  private[this] val always: Service[String, String] = Service.const(Future.value("pong"))
  private[this] val failingSvc: Service[String, String] =
    Service.const(Future.exception(new ChannelClosedException))

  private[this] def await[A](f: Future[A], timeout: Duration = timeout): A =
    Await.result(f, timeout)

  private[this] class Ctx() {

    private[this] val sr = new InMemoryStatsReceiver

    def serverAndClient(
      svc: Service[String, String]
    ): (ListeningServer, Service[String, String]) = {
      val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      val server = StringServer.server.serve(address, svc)
      val port = server.boundAddress.asInstanceOf[InetSocketAddress].getPort()
      val client = StringClient.client
        .withStack(stk =>
          stk.replace(StackClient.Role.prepFactory, ConcurrencyLimitFilter.module[String, String]))
        .configured[ConcurrencyLimitFilter.Param](ConcurrencyLimitFilter.Param
          .Configured(initialConcurrentReqLimit, maxConcurrentReqLimit))
        .withStatsReceiver(sr)
        .withRequestTimeout(timeout)
        .newService(s"localhost:$port", "testClient")

      (server, client)
    }

    def testWithServerAndClient(
      svc: Service[String, String]
    )(
      fn: Service[String, String] => Unit
    ): Unit = {
      val (server, client) = serverAndClient(svc)
      try {
        fn(client)
      } finally {
        await(Future.join(server.close(), client.close()))
      }
    }

    def requestsCounter(): Long = sr.counters(Seq("testClient", "requests"))
    def successCounter(): Long = sr.counters(Seq("testClient", "success"))
    def failuresCounter(): Long = sr.counters(Seq("testClient", "failures"))
    def droppedRequestsCounter(): Long =
      sr.counters(Seq("testClient", "concurrency_limit", "dropped_requests"))
  }

  test("Requests exceeding limit should increase dropped_request count") {
    val ctx = new Ctx
    import ctx._

    val latch = Promise[String]()
    val counter = new AtomicInteger()
    val svc = Service.mk { _: String =>
      counter.incrementAndGet()
      latch
    }

    testWithServerAndClient(svc) { client =>
      val successes = Future.collect(Seq.fill(initialConcurrentReqLimit)(client("ping")))
      eventually {
        // note that we can't check the requestsCounter because it's not incremented until
        // the response is returned to the client. Same for the `overloads` check as well.
        assert(counter.get == initialConcurrentReqLimit)
        assert(droppedRequestsCounter == 0)
      }

      val overloads = Future.collectToTry(Seq.fill(excessRps)(client("ping")))
      eventually {
        assert(counter.get == initialConcurrentReqLimit)
        assert(droppedRequestsCounter == excessRps)
      }

      // These should already be available.
      await(overloads).foreach {
        case Throw(_: ConcurrencyOverload) => // ok
        case other => fail(s"Unexpected result: $other")
      }

      // Now let our initial requests complete
      latch.setValue("pong")
      assert(await(successes) == Seq.fill(initialConcurrentReqLimit)("pong"))

      // the excessRps is not counted by request counter
      // because the filter is inserted before stats filter
      eventually {
        assert(counter.get == initialConcurrentReqLimit)
        assert(requestsCounter == initialConcurrentReqLimit)
        assert(droppedRequestsCounter == excessRps)
      }

      // one more request to make sure we're unblocked now
      assert(await(client("ping")) == "pong")
      eventually {
        assert(counter.get == initialConcurrentReqLimit + 1)
        assert(requestsCounter == initialConcurrentReqLimit + 1)
        assert(droppedRequestsCounter == excessRps)
      }
    }
  }

  test("Should not increase dropped_request count when number of requests is less than limit") {
    val ctx = new Ctx
    import ctx._
    testWithServerAndClient(always) { client =>
      val result = Future.collectToTry(Seq.fill(initialConcurrentReqLimit)(client("ping")))
      await(result)

      eventually {
        assert(successCounter == initialConcurrentReqLimit)
        assert(droppedRequestsCounter == 0)
      }
    }
  }

  test("Service application failure should not increase dropped_request count") {
    val ctx = new Ctx
    import ctx._

    testWithServerAndClient(failingSvc) { client =>
      for (_ <- 0 until initialConcurrentReqLimit) {
        intercept[ChannelClosedException] {
          await(client("ping"))
        }
      }

      eventually {
        assert(failuresCounter == initialConcurrentReqLimit)
        assert(droppedRequestsCounter == 0)
      }
    }
  }
}
