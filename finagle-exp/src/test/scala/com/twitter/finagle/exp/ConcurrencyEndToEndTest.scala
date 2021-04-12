package com.twitter.finagle.exp

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.exp.ConcurrencyLimitFilter.ConcurrencyOverload
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Duration, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite

private object ConcurrencyEndToEndTest {
  val initialConcurrentReqLimit = 10
  val maxConcurrentReqLimit = 1000
  val excessRps = 5
  // Timeout needs to be around 1.second so CI tests do not fail
  val timeout = 1.second

  val always: Service[String, String] = Service.const(Future.value("pong"))
  val failingSvc: Service[String, String] =
    Service.const(Future.exception(new ChannelClosedException))
}

class ConcurrencyEndToEndTest extends FunSuite {
  import ConcurrencyEndToEndTest._

  def await[A](f: Future[A], timeout: Duration = 5.second): A = Await.result(f, timeout)

  class Ctx() {

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
        await(Future.join(server.close(), client.close()), 10.seconds)
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

    testWithServerAndClient(always) { client =>
      val result =
        Future.collectToTry(Seq.fill(initialConcurrentReqLimit + excessRps)(client("ping")))

      try {
        await(result, 10.seconds)
      } catch {
        case _: ConcurrencyOverload =>
        case t: Throwable =>
          fail(s"expected ConcurrencyOverload, saw $t")
      }

      // the excessRps is not counted by request counter
      // because the filter is inserted before stats filter
      assert(requestsCounter == initialConcurrentReqLimit)
      assert(droppedRequestsCounter == excessRps)
    }
  }

  test("Should not increase dropped_request count when number of requests is less than limit") {
    val ctx = new Ctx
    import ctx._
    testWithServerAndClient(always) { client =>
      val result = Future.collectToTry(Seq.fill(initialConcurrentReqLimit)(client("ping")))
      await(result)
      assert(successCounter == initialConcurrentReqLimit)
      assert(droppedRequestsCounter == 0)
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
      assert(failuresCounter == initialConcurrentReqLimit)
      assert(droppedRequestsCounter == 0)
    }
  }
}
