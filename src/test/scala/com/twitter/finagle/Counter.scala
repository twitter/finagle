package com.twitter.finagle.util

import org.specs.Specification

import com.twitter.util.Time
import com.twitter.util.TimeConversions._

object CounterSpec extends Specification {
  "scalar counters" should {
    "count!" in {
      val c = new ScalarCounter
      c() must be_==(0)
      c.incr()
      c.incr()
      c() must be_==(2)

      c.incr(1000)
      c() must be_==(1002)
    }
  }

  "time windowed counter" should {
    Time.freeze()

    "keep a total sum over its window" in {
      val c = new TimeWindowedCounter(10, 10.seconds)
      c() must be_==(0)
      c.incr(1000)
      c() must be_==(1000)

      Time.advance(11.seconds)
      c.incr()
      c() must be_==(1001)

      Time.advance(80.seconds)
      c.incr()
      c() must be_==(1002)

      Time.advance(10.seconds)
      c() must be_==(2)
      c.incr()
      c() must be_==(3)
    }

    "keep a total sum over its window (2)" in {
      val c = new TimeWindowedCounter(10, 10.seconds)

      for (i <- 1 to 100) {
        c.incr()
        c() must be_==(i)
        Time.advance(1.seconds)
      }

      c() must be_==(100)

      for (i <- 0 until 10) {
        Time.advance(10.seconds)
        c.incr(10)
        c() must be_==(100)
      }
    }

    "compute rate" in {
      val c = new TimeWindowedCounter(10, 10.seconds)

      c.incr()
      c.rateInHz() must be_==(0)
      c.incr(9)
      c.rateInHz() must be_==(1)

      Time.advance(50.seconds)
      for (i <- 0 until 100) {
        c.rateInHz() must be_==(i)
        c.incr(60)
      }
    }
  }
}
