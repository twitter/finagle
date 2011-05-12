package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import org.specs.Specification
import scala.collection.JavaConversions._


//TODO after each clear Stats
object CommonsStatsReceiverSpec extends Specification {
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
  }

  "stat" should {
    "work" in {
      val stat = (new CommonsStatsReceiver()).stat("bar")

      assert(Stats.getVariable("bar_50th_percentile").read == 0.0f)
      assert(Stats.getVariable("bar_95th_percentile").read == 0.0f)
      assert(Stats.getVariable("bar_99th_percentile").read == 0.0f)

      for (i <- 0.until(10000)) {
        stat.add(i.toFloat)
      }

      //TODO find a way to poke at the stats, need to do something with a StatsModule
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