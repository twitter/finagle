package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import org.specs.Specification


object CommonsStatsReceiverSpec extends Specification {
  "counter" should {
    "return a new counter object with the given name and reflect incr operations" in {
      val counter = (new CommonsStatsReceiver()).counter("foo")
      assert (Stats.getVariable("foo").read == 0)
      counter.incr(7)
      assert (Stats.getVariable("foo").read == 7)
      counter.incr(-8)
      assert (Stats.getVariable("foo").read == -1)
    }
  }
  
  "stat" should {
    "work" in {
      val stat = (new CommonsStatsReceiver()).stat("bar")

      assert(Stats.getVariable("bar").getName == "bar")
      assert(Stats.getVariable("bar").read == 0.0f)

      stat.add(1.37f)

      assert(Stats.getVariable("bar").read == 1.37f)
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