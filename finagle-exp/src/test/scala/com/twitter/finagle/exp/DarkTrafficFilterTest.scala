package com.twitter.finagle.exp

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.finagle.Service
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util.{Await, Future, FutureCancelledException, Promise}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DarkTrafficFilterTest extends FunSuite with MockitoSugar {

  trait Fixture {
    val request = "annyang"
    val response = "hello"
    val gate = mock[() => Boolean]
    val statsReceiver = new InMemoryStatsReceiver

    val darkService = new Service[String, String] {
      val counter = statsReceiver.counter("test_applyCounts")

      override def apply(request: String) = {
        counter.incr()
        Future.value(response)
      }
    }

    val enableSampling = (s: String) => gate()

    val filter = new DarkTrafficFilter(darkService, enableSampling, statsReceiver)

    val forwarded = Seq("dark_traffic_filter", "forwarded")
    val skipped = Seq("dark_traffic_filter", "skipped")
    val failed = Seq("dark_traffic_filter", "failed")

    val service = mock[Service[String, String]]
    when(service.apply(anyObject())) thenReturn Future.value(response)
  }

  test("send light traffic for all requests") {
    new Fixture {
      when(gate()).thenReturn(false)
      assert(Await.result(filter(request, service)) == response)

      verify(service).apply(request)
      assert(statsReceiver.counters.get(Seq("test_applyCounts")) == None)

      assert(statsReceiver.counters.get(forwarded) == None)
      assert(statsReceiver.counters.get(skipped) == Some(1))
    }
  }

  test("when decider is on, send dark traffic to darkService and light to service") {
    new Fixture {
      when(gate()).thenReturn(true)
      assert(Await.result(filter(request, service)) == response)

      verify(service).apply(request)
      assert(statsReceiver.counters.get(Seq("test_applyCounts")) == Some(1))

      assert(statsReceiver.counters.get(forwarded) == Some(1))
    }
  }

  test("count failures in forwarding") {
    new Fixture {
      when(gate()).thenReturn(true)
      val failingService = new Service[String, String] {
        override def apply(request: String) = Future.exception(new Exception("fail"))
      }
      val failingFilter = new DarkTrafficFilter(failingService, enableSampling, statsReceiver)
      assert(Await.result(failingFilter(request, service)) == response)

      assert(statsReceiver.counters.get(failed) == Some(1))
    }
  }

  test("interrupts are propagated to dark traffic") {
    val lightServiceCancelled = new AtomicBoolean(false)
    val darkServiceCancelled = new AtomicBoolean(false)

    val lightPromise = new Promise[String]
    lightPromise.setInterruptHandler { case t: Throwable => lightServiceCancelled.set(true) }
    val darkPromise = new Promise[String]
    darkPromise.setInterruptHandler { case t: Throwable => darkServiceCancelled.set(true) }

    val service = Service.mk { s: String =>
      lightPromise
    }
    val darkService = Service.mk { s: String =>
      darkPromise
    }

    val filter = new DarkTrafficFilter(darkService, Function.const(true), NullStatsReceiver)
    val chainedService = filter.andThen(service)

    val lightFuture = chainedService("test")
    lightFuture.raise(new FutureCancelledException)

    assert(lightServiceCancelled.get)
    assert(darkServiceCancelled.get)
  }
}
