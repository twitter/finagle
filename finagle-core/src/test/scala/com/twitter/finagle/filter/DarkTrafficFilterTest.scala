package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps.RichDuration
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.ForwardAnnotation
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.Address
import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.FutureCancelledException
import com.twitter.util.Promise
import com.twitter.util.Time

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DarkTrafficFilterTest extends AnyFunSuite with MockitoSugar {

  def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 5.seconds)

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
      assert(statsReceiver.counters.get(Seq("test_applyCounts")) == Some(0))

      assert(statsReceiver.counters.get(forwarded) == Some(0))
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

    val service = Service.mk { s: String => lightPromise }
    val darkService = Service.mk { s: String => darkPromise }

    val filter = new DarkTrafficFilter(darkService, Function.const(true), NullStatsReceiver)
    val chainedService = filter.andThen(service)

    val lightFuture = chainedService("test")
    lightFuture.raise(new FutureCancelledException)

    assert(lightServiceCancelled.get)
    assert(darkServiceCancelled.get)
  }

  test("light and dark spans are annotated with identifier") {
    def getAnnotation(tracer: BufferingTracer, name: String): Option[Record] = {
      tracer.toSeq.find { record =>
        record.annotation match {
          case a: BinaryAnnotation if a.key == name => true
          case _ => false
        }
      }
    }

    val svc = new Service[String, String] {
      def apply(request: String): Future[String] = {
        Future(request)
      }
    }

    Time.withCurrentTimeFrozen { _ =>
      val echoServer = StringServer.server.serve(new InetSocketAddress(0), svc)

      val lightTracer = new BufferingTracer()

      val lightClient = StringClient.client
        .withTracer(lightTracer)

      val lightService = lightClient.newService(
        Name.bound(
          Address(echoServer.boundAddress
            .asInstanceOf[InetSocketAddress])),
        "light")

      val darkTracer = new BufferingTracer()
      val darkClient = StringClient.client
        .withTracer(darkTracer)

      val darkService = darkClient.newService(
        Name.bound(
          Address(echoServer.boundAddress
            .asInstanceOf[InetSocketAddress])),
        "dark")

      val darkTrafficFilter = new DarkTrafficFilter[String, String](
        darkService,
        _ => true,
        NullStatsReceiver,
        false
      )

      val service = darkTrafficFilter.andThen(lightService)

      await(service("hi"))

      assert(getAnnotation(lightTracer, "clnt/dark_request_key").isDefined)
      assert(getAnnotation(lightTracer, "clnt/is_dark_request").isEmpty)
      assert(getAnnotation(lightTracer, "clnt/has_dark_request").isDefined)

      val lightSpanId = getAnnotation(lightTracer, "clnt/dark_request_key").get.traceId.spanId

      assert(getAnnotation(darkTracer, "clnt/dark_request_key").isDefined)
      assert(getAnnotation(darkTracer, "clnt/is_dark_request").isDefined)
      assert(getAnnotation(darkTracer, "clnt/has_dark_request").isDefined)

      val darkSpanId = getAnnotation(darkTracer, "clnt/dark_request_key").get.traceId.spanId

      assert(lightSpanId != darkSpanId)
      await(service.close())
    }
  }

  for (shouldInvokeDarkTraffic <- Seq(true, false)) {
    test(
      s"dark traffic annotation `clnt/has_dark_request` is populated as $shouldInvokeDarkTraffic in light service withOUT over the wire call") {

      var darkTrafficInvoked: Any = false

      val svcDark = new Service[String, String] {
        def apply(request: String): Future[String] = {
          Future.value(request)
        }
      }

      val svcLight = new Service[String, String] {
        private def getContextAnnotation(name: String): Option[BinaryAnnotation] =
          ForwardAnnotation.current.getOrElse(Seq[BinaryAnnotation]()).find(_.key == name)

        def apply(request: String): Future[String] = {
          darkTrafficInvoked = getContextAnnotation("clnt/has_dark_request").get.value
          Future.value(request)
        }
      }

      val darkTrafficFilter = new DarkTrafficFilter[String, String](
        svcDark,
        _ => shouldInvokeDarkTraffic,
        NullStatsReceiver,
        false
      )

      val service = darkTrafficFilter.andThen(svcLight)
      await(service("hi"))
      assert(darkTrafficInvoked == shouldInvokeDarkTraffic)
      await(service.close())
    }
  }
}
