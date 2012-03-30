package com.twitter.finagle.stats

import scala.collection.mutable.HashMap

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class GlobalStatsReceiverSpec extends SpecificationWithJUnit with Mockito {
  // We seem to have to resort to these tricks-- Mockito
  // does not deal well with multiple argument lists.
  class MockReceiver extends StatsReceiver {
    val gauges = HashMap[Seq[String], (() => Float, Gauge)]()

    val repr = this
    def counter(name: String*) = null
    def stat(name: String*) = null
    def didAddGauge(name: Seq[String]) {}
    def addGauge(name: String*)(f: => Float) = {
      val gauge = mock[Gauge]
      gauges += name -> (() => f, gauge)
      didAddGauge(name); gauge
    }
  }

  "GlobalStatsReceiver" should {
    val r, r1 = spy(new MockReceiver)
    val global = new GlobalStatsReceiver

    "add gauges to receivers" in {
      "before registering" in {
        global.provideGauge("gauge") { 1F }
        global.register(r)
        there was one(r).didAddGauge(Seq("gauge"))
      }

      "after registering" in {
        global.register(r)
        there was no(r).didAddGauge(any)
        global.provideGauge("gauge") { 1F }
        there was one(r).didAddGauge(Seq("gauge"))
      }
    }

    "register one per repl" in {
      global.provideGauge("gauge") { 1F }
      global.register(r)
      there was one(r).didAddGauge(Seq("gauge"))
      global.register(r)
      there was one(r).didAddGauge(Seq("gauge"))
    }

    "propagates the correct gauges" in {
      var count = 0
      global.provideGauge("gauge") { count += 1; count.toFloat }
      count must be_==(0)

      r.gauges must beEmpty
      global.register(r)
      count must be_==(0)
      r.gauges must haveSize(1)
      val (f, _) = r.gauges(Seq("gauge"))
      f() must be_==(1)
      count must be_==(1)
    }

    "propagates removal" in {
      global.register(r)
      r.gauges must beEmpty
      val ggauge = global.addGauge("ok") { 1F }
      r.gauges must haveSize(1)
      val (_, rgauge) = r.gauges(Seq("ok"))
      there was no(rgauge).remove()
      ggauge.remove()
      there was one(rgauge).remove()
    }
  }
}
