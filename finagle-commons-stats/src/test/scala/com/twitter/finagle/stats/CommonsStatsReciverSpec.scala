package com.twitter.finagle.stats

import com.twitter.common.stats.Stats
import org.specs.Specification


object CommonsStatsReceiverSpec extends Specification {
  "counter" should {
    "return a new counter object with the given name" in {
      val counter = (new CommonsStatsReceiver()).counter("foo")
      assert (Stats.getVariable("foo").read == 0)
      counter.incr(7)
      assert (Stats.getVariable("foo").read == 7)
      counter.incr(-8)
      assert (Stats.getVariable("foo").read == -1)
    }
  }
  
}