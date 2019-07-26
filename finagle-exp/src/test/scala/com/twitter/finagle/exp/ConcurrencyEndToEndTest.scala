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

class ConcurrencyEndToEndTest extends FunSuite {
  def await[A](f: Future[A], timeout: Duration = 5.second): A = Await.result(f, timeout)

  class Ctx() {
    val initialConcurrentReqLimit = 10
    val maxConcurrentReqLimit = 1000
    val excessRps = 5
    // Timeout needs to be around 500.milliseconds so CI tests do not fail
    val timeout = 500.millisecond

    val always: Service[String, String] = Service.const(Future.value("pong"))
    val failingSvc: Service[String, String] =
      Service.const(Future.exception(new ChannelClosedException))
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val sr = new InMemoryStatsReceiver

    def serverAndClient(
      svc: Service[String, String]
    ): (ListeningServer, Service[String, String]) = {
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
    def requestsCounter(): Long = sr.counters(Seq("testClient", "requests"))
    def successCounter(): Long = sr.counters(Seq("testClient", "success"))
    def failuresCounter(): Long = sr.counters(Seq("testClient", "failures"))
    def droppedRequestsCounter(): Long =
      sr.counters(Seq("testClient", "concurrency_limit", "dropped_requests"))
    def limitGauge(): () => Float =
      sr.gauges(Seq("testClient", "concurrency_limit", "estimated_concurrency_limit"))
  }

  test("Requests exceeding limit should increase dropped_request count") {
    val ctx = new Ctx
    import ctx._
    val (testServer, testClient) = serverAndClient(always)
    val result =
      Future.collectToTry(Seq.fill(initialConcurrentReqLimit + excessRps)(testClient("ping")))
    try {
      await(result, 10.seconds)
    } catch {
      case _: ConcurrencyOverload =>
      case t: Throwable =>
        fail(s"expected ConcurrencyOverload, saw $t")
    }
    await(testServer.close(), 10.second)

    // the excessRps is not counted by request counter
    // because the filter is inserted before stats filter
    assert(requestsCounter == initialConcurrentReqLimit)
    assert(limitGauge()() > initialConcurrentReqLimit)
    assert(droppedRequestsCounter == excessRps)
    await(testClient.close())
  }

  test("Should not increase dropped_request count when number of requests is less than limit") {
    val ctx = new Ctx
    import ctx._
    val (testServer, testClient) = serverAndClient(always)
    val result = Future.collectToTry(Seq.fill(initialConcurrentReqLimit)(testClient("ping")))
    await(result)

    assert(successCounter == initialConcurrentReqLimit)
    assert(droppedRequestsCounter == 0)
    assert(limitGauge()() >= initialConcurrentReqLimit)
    await(testClient.close())
    await(testServer.close())
  }

  test("Service application failure should not increase dropped_request count") {
    val ctx = new Ctx
    import ctx._

    val (testServer, testClient) = serverAndClient(failingSvc)
    for (_ <- 0 until initialConcurrentReqLimit) {
      intercept[ChannelClosedException] {
        await(testClient("ping"))
      }
    }
    await(testServer.close())
    assert(failuresCounter == initialConcurrentReqLimit)
    assert(droppedRequestsCounter == 0)
    assert(limitGauge()() < initialConcurrentReqLimit)
    await(testClient.close())
  }
}
