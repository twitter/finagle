package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.stats.exp.ExpressionSchemaKey
import com.twitter.finagle.stats.exp.FunctionExpression
import com.twitter.finagle.stats.exp.HistogramExpression
import com.twitter.finagle.stats.exp.MetricExpression
import com.twitter.finagle.stats._
import com.twitter.util._
import java.util.concurrent.TimeUnit
import org.scalatest.funsuite.AnyFunSuite

class StatsFilterTest extends AnyFunSuite {
  val BasicExceptions = new CategorizingExceptionStatsHandler(_ => None, _ => None, rollup = false)

  def getService(
    exceptionStatsHandler: ExceptionStatsHandler = BasicExceptions
  ): (Promise[String], InMemoryStatsReceiver, Service[String, String]) = {
    val receiver = new InMemoryStatsReceiver()
    val metricBuilderRegistry = new MetricBuilderRegistry()
    val statsFilter =
      new StatsFilter[String, String](receiver, exceptionStatsHandler, metricBuilderRegistry)
    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String): Future[String] = promise
    }

    (promise, receiver, statsFilter.andThen(service))
  }

  private[this] def nameToKey(
    name: String,
    labels: Map[String, String] = Map()
  ): ExpressionSchemaKey =
    ExpressionSchemaKey(name, labels, Seq())

  test("latency stat defaults to milliseconds") {
    val sr = new InMemoryStatsReceiver()
    val filter = new StatsFilter[String, String](
      sr,
      ResponseClassifier.Default,
      StatsFilter.DefaultExceptions,
      TimeUnit.MILLISECONDS,
      Stopwatch.timeMillis)
    val promise = new Promise[String]
    val svc = filter.andThen(new Service[String, String] {
      def apply(request: String): Promise[String] = promise
    })

    Time.withCurrentTimeFrozen { tc =>
      svc("1")
      tc.advance(100.millis)
      promise.setValue("done")
      assert(sr.stat("request_latency_ms")() == Seq(100))
    }
  }

  test("latency stat in microseconds") {
    val sr = new InMemoryStatsReceiver()
    val filter =
      new StatsFilter[String, String](
        sr,
        ResponseClassifier.Default,
        StatsFilter.DefaultExceptions,
        TimeUnit.MICROSECONDS,
        Stopwatch.timeMicros)
    val promise = new Promise[String]
    val svc = filter.andThen(new Service[String, String] {
      def apply(request: String): Promise[String] = promise
    })

    Time.withCurrentTimeFrozen { tc =>
      svc("1")
      tc.advance(100.millis)
      promise.setValue("done")
      assert(sr.stat("request_latency_us")() == Seq(100.millis.inMicroseconds))
    }
  }

  test("latency stat in seconds") {
    val sr = new InMemoryStatsReceiver()
    val timeSeconds: () => Long = () => Time.now.inSeconds
    val filter =
      new StatsFilter[String, String](
        sr,
        ResponseClassifier.Default,
        StatsFilter.DefaultExceptions,
        TimeUnit.SECONDS,
        timeSeconds)
    val promise = new Promise[String]
    val svc = filter.andThen(new Service[String, String] {
      def apply(request: String): Promise[String] = promise
    })

    Time.withCurrentTimeFrozen { tc =>
      svc("1")
      tc.advance(22.seconds)
      promise.setValue("done")
      assert(sr.stat("request_latency_secs")() == Seq(22))
    }
  }

  test("report exceptions") {
    val (promise, receiver, statsService) = getService()

    val e1 = new Exception("e1")
    val e2 = new RequestException(e1)
    val e3 = WriteException(e2)
    e3.serviceName = "bogus"
    promise.setException(e3)
    val res = statsService("foo")
    assert(res.isDefined)
    assert(Await.ready(res).poll.get.isThrow)

    val sourced = receiver.counters.filterKeys { _.exists(_ == "sourcedfailures") }
    assert(sourced.size == 0)

    val unsourced = receiver.counters.filterKeys { _.exists(_ == "failures") }
    assert(unsourced.size == 2)
    assert(unsourced(Seq("failures")) == 1)
    assert(
      unsourced(
        Seq(
          "failures",
          classOf[ChannelWriteException].getName(),
          classOf[RequestException].getName(),
          classOf[Exception].getName()
        )
      ) == 1
    )
  }

  test("source failures") {
    val esh = new CategorizingExceptionStatsHandler(sourceFunction = _ => Some("bogus"))

    val (promise, receiver, statsService) = getService(esh)
    val e = new Failure("e").withSource(Failure.Source.Service, "bogus")
    promise.setException(e)
    val res = statsService("foo")
    assert(res.isDefined)
    assert(Await.ready(res).poll.get.isThrow)

    val sourced = receiver.counters.filterKeys { _.exists(_ == "sourcedfailures") }
    assert(sourced.size == 2)
    assert(sourced(Seq("sourcedfailures", "bogus")) == 1)
    assert(sourced(Seq("sourcedfailures", "bogus", classOf[Failure].getName())) == 1)

    val unsourced = receiver.counters.filterKeys { _.exists(_ == "failures") }
    assert(unsourced.size == 2)
    assert(unsourced(Seq("failures")) == 1)
    assert(unsourced(Seq("failures", classOf[Failure].getName())) == 1)
  }

  test("don't report failures flagged FailureFlags.Ignorable") {
    val (promise, receiver, statsService) = getService()

    assert(receiver.counters(Seq("requests")) == 0)
    assert(!receiver.counters.keys.exists(_.contains("failure")))
    statsService("foo")

    assert(receiver.gauges(Seq("pending"))() == 1.0)
    promise.setException(Failure.ignorable("Ignore me (disappear)."))

    assert(!receiver.counters.keys.exists(_.contains("failure")))
    assert(receiver.counters(Seq("requests")) == 0)
    assert(receiver.counters(Seq("success")) == 0)
    assert(receiver.gauges(Seq("pending"))() == 0.0)
  }

  test("report pending requests on success") {
    val (promise, receiver, statsService) = getService()
    assert(receiver.gauges(Seq("pending"))() == 0.0)
    statsService("foo")
    assert(receiver.gauges(Seq("pending"))() == 1.0)
    promise.setValue("")
    assert(receiver.gauges(Seq("pending"))() == 0.0)
  }

  test("report pending requests on failure") {
    val (promise, receiver, statsService) = getService()
    assert(receiver.gauges(Seq("pending"))() == 0.0)
    statsService("foo")
    assert(receiver.gauges(Seq("pending"))() == 1.0)
    promise.setException(new Exception)
    assert(receiver.gauges(Seq("pending"))() == 0.0)
  }

  test("don't report pending requests after uncaught exceptions") {
    val receiver = new InMemoryStatsReceiver()
    val service = new Service[String, String] {
      def apply(request: String): Future[String] = throw new IllegalStateException("broken")
    }

    val statsFilter = new StatsFilter[String, String](receiver, BasicExceptions)

    // verifies that before the Exception is thrown, the pending metric in the StatsFilter is incremented to 1
    val verifyingFilter = new SimpleFilter[String, String] {
      private val incremented = receiver.counter("incremented")
      override def apply(request: String, service: Service[String, String]): Future[String] = {
        val pendingRequests = receiver.gauges(Seq("pending"))().toInt
        incremented.incr(pendingRequests)
        service(request)
      }
    }

    // not chaining using andThen here because that wraps any raw Exception inside a Future.exception
    val chain = new Service[String, String] {
      def apply(request: String): Future[String] =
        statsFilter.apply(
          request,
          new Service[String, String] {
            def apply(req: String): Future[String] = verifyingFilter.apply(req, service)
          })
    }

    assert(receiver.gauges(Seq("pending"))() == 0.0)
    intercept[IllegalStateException] {
      Await.result(chain("foo"))
    }
    assert(receiver.gauges(Seq("pending"))() == 0.0)

    assert(receiver.counter("incremented")() == 1)
  }

  test("should count failure requests only after they are finished") {
    val (promise, receiver, statsService) = getService()

    assert(receiver.counters(Seq("requests")) == 0)
    assert(receiver.counters(Seq("failures")) == 0)

    val f = statsService("foo")

    assert(receiver.counters(Seq("requests")) == 0)
    assert(receiver.counters(Seq("failures")) == 0)

    promise.setException(new Exception)

    assert(receiver.counters(Seq("requests")) == 1)
    assert(receiver.counters(Seq("failures")) == 1)
  }

  test("should count successful requests only after they are finished") {
    val (promise, receiver, statsService) = getService()

    assert(receiver.counters(Seq("requests")) == 0)
    assert(receiver.counters(Seq("failures")) == 0)

    val f = statsService("foo")

    assert(receiver.counters(Seq("requests")) == 0)
    assert(receiver.counters(Seq("failures")) == 0)

    promise.setValue("whatever")

    assert(receiver.counters(Seq("requests")) == 1)
    assert(receiver.counters(Seq("success")) == 1)
  }

  test("support rollup exceptions") {
    val esh = new CategorizingExceptionStatsHandler(rollup = true)

    val (promise, receiver, statsService) = getService(esh)

    val e = ChannelWriteException(new Exception("e1"))
    promise.setException(e)
    val res = statsService("foo")

    val unsourced = receiver.counters.filterKeys { _.exists(_ == "failures") }

    assert(unsourced.size == 3)
    assert(unsourced(Seq("failures")) == 1)
    assert(unsourced(Seq("failures", classOf[ChannelWriteException].getName())) == 1)
    assert(
      unsourced(
        Seq("failures", classOf[ChannelWriteException].getName(), classOf[Exception].getName())
      ) == 1
    )
  }

  test("respects ResponseClassifier") {
    val sr = new InMemoryStatsReceiver()
    val svc = Service.mk { i: Int =>
      if (i < 0) Future.exception(new RuntimeException(i.toString))
      else Future(i)
    }
    val aClassifier: ResponseClassifier = {
      case ReqRep(_, Return(i: Int)) if i == 5 => ResponseClass.RetryableFailure
      case ReqRep(_, Throw(x)) if x.getMessage == "-5" => ResponseClass.Success
    }
    val statsFilter = new StatsFilter[Int, Int](
      sr,
      aClassifier,
      StatsFilter.DefaultExceptions,
      TimeUnit.MILLISECONDS
    )

    val service = statsFilter.andThen(svc)

    // able to categorize Returns as failures
    assert(5 == Await.result(service(5), 1.second))
    assert(1 == sr.counter("requests")())
    assert(0 == sr.counter("success")())
    assert(1 == sr.counter("failures")())
    val failure =
      sr.counter("failures", "com.twitter.finagle.service.ResponseClassificationSyntheticException")
    assert(1 == failure())

    // able to categorize Throws as success
    intercept[RuntimeException] { Await.result(service(-5), 1.second) }
    assert(2 == sr.counter("requests")())
    assert(1 == sr.counter("success")())
    assert(1 == sr.counter("failures")())

    // handles responses that are not defined in our classifier
    assert(!aClassifier.isDefinedAt(ReqRep(3, Return(1))))
    assert(3 == Await.result(service(3), 1.second))
    assert(3 == sr.counter("requests")())
    assert(2 == sr.counter("success")())
    assert(1 == sr.counter("failures")())
  }

  test("expressions are instrumented") {
    val (_, receiver, _) = getService()

    assert(receiver.expressions(nameToKey("success_rate")).expr.isInstanceOf[FunctionExpression])
    assert(receiver.expressions(nameToKey("throughput")).expr.isInstanceOf[MetricExpression])
    assert(
      receiver
        .expressions(nameToKey("latency", Map("bucket" -> "p99"))).expr.isInstanceOf[
          HistogramExpression])
  }

  test("standard metrics are instrumented") {
    val sr1 = new InMemoryStatsReceiver()
    val sr2 = new InMemoryStatsReceiver()
    val builtinSr = new InMemoryStatsReceiver()

    val svc = Service.mk { i: Int =>
      if (i < 0) Future.exception(new RuntimeException(i.toString))
      else Future(i)
    }

    StandardStatsReceiver.serverCount.set(0)
    LoadedStatsReceiver.self = builtinSr
    def standardStats(protoName: String) =
      StatsOnly(new StandardStatsReceiver(stats.Server, protoName))

    def statsFilter(configuredSr: StatsReceiver, protoName: String) = new StatsFilter[Int, Int](
      statsReceiver = configuredSr,
      responseClassifier = ResponseClassifier.Default,
      exceptionStatsHandler = StatsFilter.DefaultExceptions,
      timeUnit = TimeUnit.MILLISECONDS,
      now = Stopwatch.systemMillis,
      metricsRegistry = None,
      standardStats = standardStats(protoName))

    val service1 = statsFilter(sr1, "thriftmux").andThen(svc)
    val service2 = statsFilter(sr2, "http").andThen(svc)

    assert(5 == Await.result(service1(5), 1.second))
    assert(5 == Await.result(service2(5), 1.second))

    assert(1 == sr1.counter("requests")())
    assert(
      1 == builtinSr
        .counter("standard-service-metric-v1", "srv", "thriftmux", "server-0", "requests")())
    assert(1 == sr1.counter("success")())
    assert(
      1 == builtinSr
        .counter("standard-service-metric-v1", "srv", "thriftmux", "server-0", "success")())
    assert(0 == sr1.counter("failures")())
    assert(
      0 == builtinSr
        .counter("standard-service-metric-v1", "srv", "thriftmux", "server-0", "failures")())

    //overall requests
    assert(2 == builtinSr.counter("standard-service-metric-v1", "srv", "requests")())

    assert(1 == sr2.counter("requests")())
    assert(
      1 == builtinSr.counter("standard-service-metric-v1", "srv", "http", "server-1", "requests")())
  }

  test("standard metrics respects a different ResponseClassifier") {
    val sr = new InMemoryStatsReceiver()
    val svc = Service.mk { i: Int =>
      if (i < 0) Future.exception(new RuntimeException(i.toString))
      else Future(i)
    }

    val aClassifier: ResponseClassifier = {
      case ReqRep(_, Return(i: Int)) if i == 5 => ResponseClass.RetryableFailure
      case ReqRep(_, Throw(x)) if x.getMessage == "-5" => ResponseClass.Success
    }

    StandardStatsReceiver.serverCount.set(0)
    LoadedStatsReceiver.self = sr
    val standardStats =
      StatsAndClassifier(new StandardStatsReceiver(stats.Server, "thriftmux"), aClassifier)

    val statsFilter = new StatsFilter[Int, Int](
      statsReceiver = sr,
      responseClassifier = ResponseClassifier.Default,
      exceptionStatsHandler = StatsFilter.DefaultExceptions,
      timeUnit = TimeUnit.MILLISECONDS,
      now = Stopwatch.systemMillis,
      metricsRegistry = None,
      standardStats = standardStats)

    val service = statsFilter.andThen(svc)

    assert(5 == Await.result(service(5), 1.second))
    assert(1 == sr.counter("requests")())
    assert(
      1 == sr.counter("standard-service-metric-v1", "srv", "thriftmux", "server-0", "requests")())
    assert(1 == sr.counter("success")())
    assert(
      0 == sr.counter("standard-service-metric-v1", "srv", "thriftmux", "server-0", "success")())
    assert(0 == sr.counter("failures")())
    assert(
      1 == sr.counter("standard-service-metric-v1", "srv", "thriftmux", "server-0", "failures")())

    val stdFailure =
      sr.counter(
        "standard-service-metric-v1",
        "srv",
        "thriftmux",
        "server-0",
        "failures",
        "com.twitter.finagle.service.ResponseClassificationSyntheticException")
    assert(1 == stdFailure())
  }
}
