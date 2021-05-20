package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Stopwatch, Time, Future}
import com.twitter.conversions.DurationOps._
import org.scalatest.funsuite.AnyFunSuite

class ServerStatsFilterTest extends AnyFunSuite {
  test("Records handletime for a service") {
    Time.withCurrentTimeFrozen { ctl =>
      val inMemory = new InMemoryStatsReceiver
      val svc = Service.mk[Unit, Unit] { unit =>
        ctl.advance(5.microseconds)
        Future.never
      }
      val filter = new ServerStatsFilter[Unit, Unit](inMemory, Stopwatch.timeNanos)
      filter.andThen(svc)(())
      val expected = 5
      val actual = inMemory.stats(Seq("handletime_us"))(0)
      assert(actual == expected)
    }
  }
}
