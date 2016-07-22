package com.twitter.finagle.exp

import com.twitter.concurrent.LocalScheduler
import com.twitter.finagle.stats.{Gauge, InMemoryStatsReceiver}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class FinagleSchedulerTest extends FunSuite {

  test("addGauges") {
    val stats = new InMemoryStatsReceiver()
    val scheduler = new LocalScheduler()
    val gauges = new mutable.MutableList[Gauge]()

    FinagleScheduler.addGauges(scheduler, stats, gauges)
    assert(2 == gauges.size)
    assert(2 == stats.gauges.size)
  }

}
