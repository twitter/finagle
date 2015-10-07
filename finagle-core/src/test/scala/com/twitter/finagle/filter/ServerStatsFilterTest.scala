package com.twitter.finagle.filter

import com.twitter.finagle.{Deadline, Service}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Stopwatch, Time, Future}
import com.twitter.conversions.time._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ServerStatsFilterTest extends FunSuite {
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

  test("Records budget remaining for a service") {
    Time.withCurrentTimeFrozen { ctl =>
      val inMemory = new InMemoryStatsReceiver
      Contexts.broadcast.let(Deadline, Deadline(Time.now, Time.now + 15.milliseconds)) {
        ctl.advance(5.milliseconds)
        val svc = Service.mk[Unit, Unit] { unit => Future.never }
        val filter = new ServerStatsFilter[Unit, Unit](inMemory, Stopwatch.timeNanos)
        filter.andThen(svc)(())
        val expected = 10f
        val actual = inMemory.stats(Seq("deadline_budget_ms"))(0)
        assert(actual == expected)
      }
    }
  }

  test("Records transit time for a service") {
    Time.withCurrentTimeFrozen { ctl =>
      val inMemory = new InMemoryStatsReceiver
      Contexts.broadcast.let(Deadline, Deadline(Time.now, Time.now + 15.milliseconds)) {
        ctl.advance(5.milliseconds)
        val svc = Service.mk[Unit, Unit] { unit => Future.never }
        val filter = new ServerStatsFilter[Unit, Unit](inMemory, Stopwatch.timeNanos)
        filter.andThen(svc)(())
        val expected = 5f
        val actual = inMemory.stats(Seq("transit_latency_ms"))(0)
        assert(actual == expected)
      }
    }
  }
}
