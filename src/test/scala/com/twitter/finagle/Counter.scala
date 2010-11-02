package com.twitter.finagle.util

import org.specs.Specification

import com.twitter.util.Time
import com.twitter.util.TimeConversions._

object StatisticSpec extends Specification {
  "scalar statistics" should {
    "count!" in {
      val c = new ScalarStatistic
      c.sum must be_==(0)
      c.add(1)
      c.add(1)
      c.sum must be_==(2)

      c.add(1, 1000)
      c.sum must be_==(1002)
    }
  }

  "time windowed counter" should {
    Time.freeze()

    "keep a total sum over its window" in {
      val c = new TimeWindowedStatistic[ScalarStatistic](10, 10.seconds)
      c.sum must be_==(0)
      c.add(1, 1000)
      c.sum must be_==(1000)

      Time.advance(11.seconds)
      c.add(1)
      c.sum must be_==(1001)

      Time.advance(80.seconds)
      c.add(1)
      c.sum must be_==(1002)

      Time.advance(10.seconds)
      c.sum must be_==(2)
      c.add(1)
      c.sum must be_==(3)
    }

    "keep a total sum over its window (2)" in {
      val c = new TimeWindowedStatistic[ScalarStatistic](10, 10.seconds)

      for (i <- 1 to 100) {
        c.add(1)
        c.sum must be_==(i)
        Time.advance(1.seconds)
      }

      c.sum must be_==(100)

      for (i <- 0 until 10) {
        Time.advance(10.seconds)
        c.add(10)
        c.sum must be_==(100)
      }
    }

    "compute rate" in {
      val c = new TimeWindowedStatistic(10, 10.seconds)
      println(1, c)
      c.add(1)
      println(2, c)
      c.rateInHz() must be_==(1/10.0)
      println(3, c)
      Time.advance(50.seconds)
      c.add(1)
      println(4, c)
      c.rateInHz() must be_==(1)

      Time.advance(50.seconds)
      for (i <- 0 until 100) {
        c.rateInHz() must be_==(i)
        c.add(60)
      }
    }
  }
}
