package com.twitter.finagle.exp

import com.twitter.concurrent.{LocalScheduler, Scheduler}
import com.twitter.finagle.stats.{Gauge, InMemoryStatsReceiver}
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class FinagleSchedulerTest extends AnyFunSuite {

  test("addGauges") {
    val stats = new InMemoryStatsReceiver()
    val scheduler = new LocalScheduler()
    val gauges = new mutable.ArrayBuffer[Gauge]()

    FinagleScheduler.addGauges(scheduler, stats, gauges)
    assert(2 == gauges.size)
    assert(2 == stats.gauges.size)
  }

  test("supports a service loaded scheduler") {
    val scheduler = new LocalScheduler()
    var params = List("a", "b")
    val service = new FinagleSchedulerService {
      def paramsFormat = ""
      def create(p: List[String]) = {
        assert(params == p)
        Some(scheduler)
      }
    }
    val prevScheduler = Scheduler()
    try {
      new BaseFinagleScheduler(params.mkString(":"), () => List(service)).init()
      assert(Scheduler() == scheduler)
    } finally {
      Scheduler.setUnsafe(prevScheduler)
    }
  }

  test("fails if there are multiple service loaded schedulers") {
    def service = new FinagleSchedulerService {
      def paramsFormat = ""
      def create(p: List[String]) = Some(new LocalScheduler())
    }
    intercept[IllegalArgumentException] {
      new BaseFinagleScheduler("", () => List(service, service)).init()
    }
  }

}
