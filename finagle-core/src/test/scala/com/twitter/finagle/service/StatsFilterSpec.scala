package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.util.Promise
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{SourcedException, RequestException, WriteException, Service}

class StatsFilterSpec extends SpecificationWithJUnit with Mockito {
  "Stats Filter" should {
    val receiver = new InMemoryStatsReceiver()
    val statsFilter = new StatsFilter[String, String](receiver)
    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }
    val statsService = statsFilter andThen service

    "report exceptions" in {
      val e1 = new Exception("e1")
      val e2 = new RequestException(e1)
      val e3 = new WriteException(e2)
      e3.serviceName = "bogus"
      promise.setException(e3)
      val res = statsService("foo")
      res.isDefined must beTrue
      res.isThrow must beTrue
      val sourced = receiver.counters.keys.filter { _.exists(_ == "sourcedfailures") }
      sourced.size must be_==(1)
      sourced.toSeq(0).exists(_.indexOf("bogus") >=0)
      val unsourced = receiver.counters.keys.filter { _.exists(_ == "failures") }
      unsourced.size must be_==(1)
      unsourced.toSeq(0).exists { s => s.indexOf("RequestException") >= 0 && s.indexOf("WriteException") >= 0 }
    }
  }
}