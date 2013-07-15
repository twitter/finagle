package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import org.specs.SpecificationWithJUnit

//TODO after each clear Stats
class CommonsStatsReceiverSpec extends SpecificationWithJUnit {
  doAfter{
    Stats.flush
  }

  "counter" should {
    "return a new counter object with the given name and reflect incr operations" in {
      val counter = (new CommonsStatsReceiver()).counter("foo")
      assert (Stats.getVariable("foo").read == 0)
      counter.incr(7)
      assert (Stats.getVariable("foo").read == 7)
      counter.incr(-8)
      assert (Stats.getVariable("foo").read == -1)
    }

    "memoize objects with the same name" in {
      val sr = new CommonsStatsReceiver()
      sr.counter("one") must be(sr.counter("one"))
      sr.counter("one", "two") must be(sr.counter("one", "two"))

      sr.counter("one") mustNot be(sr.counter("one", "two"))
    }
  }

  "stat" should {
    "work" in {
      val stat = (new CommonsStatsReceiver()).stat("bar")

      assert(Stats.getVariable("bar_50_0_percentile").read == 0.0f)
      assert(Stats.getVariable("bar_95_0_percentile").read == 0.0f)
      assert(Stats.getVariable("bar_99_0_percentile").read == 0.0f)

      for (i <- 0.until(10000)) {
        stat.add(i.toFloat)
      }

      //TODO find a way to poke at the stats, need to do something with a StatsModule
    }

    "should be memoized" in {
      val receiver = new CommonsStatsReceiver()
      val stat1 = receiver.stat("what")
      val stat2 = receiver.stat("what")
      stat1 must be(stat2)
    }
  }

  "addGauge" should {
    "work" in {
      var inner = 0.0f
      (new CommonsStatsReceiver).addGauge("bam") {
        inner
      }
      assert(Stats.getVariable("bam").read == 0.0f)
      inner = 3.14f
      assert(Stats.getVariable("bam").read == 3.14f)
    }
  }

}
