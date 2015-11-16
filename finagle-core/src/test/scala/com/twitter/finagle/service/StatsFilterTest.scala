package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{CategorizingExceptionStatsHandler, ExceptionStatsHandler, InMemoryStatsReceiver}
import com.twitter.finagle._
import com.twitter.util.{Time, Await, Promise}
import java.util.concurrent.TimeUnit
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatsFilterTest extends FunSuite {
  val BasicExceptions = new CategorizingExceptionStatsHandler(_ => None, _ => None, rollup = false)

  def getService(
    exceptionStatsHandler: ExceptionStatsHandler = BasicExceptions
  ): (Promise[String], InMemoryStatsReceiver, Service[String, String]) = {
    val receiver = new InMemoryStatsReceiver()
    val statsFilter = new StatsFilter[String, String](receiver, exceptionStatsHandler)
    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }

    (promise, receiver, statsFilter andThen service)
  }

  test("latency stat defaults to milliseconds") {
    val sr = new InMemoryStatsReceiver()
    val filter = new StatsFilter[String, String](sr)
    val promise = new Promise[String]
    val svc = filter andThen new Service[String, String] {
      def apply(request: String) = promise
    }

    Time.withCurrentTimeFrozen { tc =>
      svc("1")
      tc.advance(100.millis)
      promise.setValue("done")
      assert(sr.stat("request_latency_ms")() == Seq(100))
    }
  }

  test("latency stat in microseconds") {
    val sr = new InMemoryStatsReceiver()
    val filter = new StatsFilter[String, String](
      sr, StatsFilter.DefaultExceptions, TimeUnit.MICROSECONDS)
    val promise = new Promise[String]
    val svc = filter andThen new Service[String, String] {
      def apply(request: String) = promise
    }

    Time.withCurrentTimeFrozen { tc =>
      svc("1")
      tc.advance(100.millis)
      promise.setValue("done")
      assert(sr.stat("request_latency_us")() == Seq(100.millis.inMicroseconds))
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
    assert(unsourced(Seq("failures", classOf[ChannelWriteException].getName(),
      classOf[RequestException].getName(), classOf[Exception].getName())) == 1)
  }

  test("source failures") {
    val esh = new CategorizingExceptionStatsHandler(
      sourceFunction = _ => Some("bogus"))

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

  test("don't report BackupRequestLost exceptions") {
    for (exc <- Seq(BackupRequestLost, WriteException(BackupRequestLost))) {
      val (promise, receiver, statsService) = getService()

      // It may seem strange to test for the absence
      // of these keys, but StatsReceiver semantics are
      // lazy: they are accessed only when incremented.

      assert(!receiver.counters.contains(Seq("requests")))
      assert(!receiver.counters.keys.exists(_ contains "failure"))
      statsService("foo")
      assert(receiver.gauges(Seq("pending"))() == 1.0)
      promise.setException(BackupRequestLost)
      assert(!receiver.counters.keys.exists(_ contains "failure"))
      assert(!receiver.counters.contains(Seq("requests")))
      assert(!receiver.counters.contains(Seq("success")))
      assert(receiver.gauges(Seq("pending"))() == 0.0)
    }
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

  test("should count failure requests only after they are finished") {
    val (promise, receiver, statsService) = getService()

    assert(receiver.counters.contains(Seq("requests")) == false)
    assert(receiver.counters.contains(Seq("failures")) == false)

    val f = statsService("foo")

    assert(receiver.counters.contains(Seq("requests")) == false)
    assert(receiver.counters.contains(Seq("failures")) == false)

    promise.setException(new Exception)

    assert(receiver.counters(Seq("requests")) == 1)
    assert(receiver.counters(Seq("failures")) == 1)
  }

  test("should count successful requests only after they are finished") {
    val (promise, receiver, statsService) = getService()

    assert(receiver.counters.contains(Seq("requests")) == false)
    assert(receiver.counters.contains(Seq("failures")) == false)

    val f = statsService("foo")

    assert(receiver.counters.contains(Seq("requests")) == false)
    assert(receiver.counters.contains(Seq("failures")) == false)

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
    assert(unsourced(Seq("failures", classOf[ChannelWriteException].getName(), classOf[Exception].getName())) == 1)
  }
}
