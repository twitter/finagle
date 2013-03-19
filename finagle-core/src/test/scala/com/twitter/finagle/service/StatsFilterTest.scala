package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.util.Promise
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{RequestException, WriteException, Service}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatsFilterTest extends FunSuite {
  def getService: (Promise[String], InMemoryStatsReceiver, Service[String, String]) = {
    val receiver = new InMemoryStatsReceiver()
    val statsFilter = new StatsFilter[String, String](receiver)
    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }

    (promise, receiver, statsFilter andThen service)
  }

  test("report exceptions") {
    val (promise, receiver, statsService) = getService

    val e1 = new Exception("e1")
    val e2 = new RequestException(e1)
    val e3 = WriteException(e2)
    e3.serviceName = "bogus"
    promise.setException(e3)
    val res = statsService("foo")
    assert(res.isDefined)
    assert(res.isThrow)
    val sourced = receiver.counters.keys.filter { _.exists(_ == "sourcedfailures") }
    assert(sourced.size == 1)
    assert(sourced.toSeq(0).exists(_.indexOf("bogus") >=0))
    val unsourced = receiver.counters.keys.filter { _.exists(_ == "failures") }
    assert(unsourced.size == 1)
    assert(unsourced.toSeq(0).exists { s => s.indexOf("RequestException") >= 0 })
    assert(unsourced.toSeq(0).exists { s => s.indexOf("WriteException") >= 0 })
  }

  test("report pending requests on success") {
    val (promise, receiver, statsService) = getService
    assert(receiver.gauges(Seq("pending"))() == 0.0)
    statsService("foo")
    assert(receiver.gauges(Seq("pending"))() == 1.0)
    promise.setValue("")
    assert(receiver.gauges(Seq("pending"))() == 0.0)
  }

  test("report pending requests on failure") {
    val (promise, receiver, statsService) = getService
    assert(receiver.gauges(Seq("pending"))() == 0.0)
    statsService("foo")
    assert(receiver.gauges(Seq("pending"))() == 1.0)
    promise.setException(new Exception)
    assert(receiver.gauges(Seq("pending"))() == 0.0)
  }
}
