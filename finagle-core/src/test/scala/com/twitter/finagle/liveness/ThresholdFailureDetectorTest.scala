package com.twitter.finagle.liveness

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Status
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.Outcome
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatest.funsuite.FixtureAnyFunSuite

class ThresholdFailureDetectorTest
    extends FixtureAnyFunSuite
    with AssertionsForJUnit
    with Eventually
    with IntegrationPatience {

  type FixtureParam = TimeControl

  protected def withFixture(
    test: OneArgTest
  ): Outcome = {
    Time.withCurrentTimeFrozen { timeControl =>
      withFixture(test.toNoArgTest(timeControl))
    }
  }

  private class Ctx(closeTimeout: Duration = 1000.milliseconds) {
    val n = new AtomicInteger(0)
    val latch = new Latch

    def ping(): Future[Unit] = {
      n.incrementAndGet()
      latch.get
    }

    def nanoTime(): Long = Time.now.inNanoseconds
    val sr = new InMemoryStatsReceiver

    val timer = new MockTimer
    val d = new ThresholdFailureDetector(
      ping _,
      minPeriod = 10.milliseconds,
      closeTimeout = closeTimeout,
      nanoTime = nanoTime _,
      statsReceiver = sr,
      timer = timer
    )
  }

  test("pings every minPeriod") { tc =>
    val ctx = new Ctx
    import ctx._

    assert(d.status == Status.Open)

    for (i <- Seq.range(1, 10)) {
      assert(n.get == i)
      tc.advance(10.milliseconds)
      timer.tick()
      latch.flip()
      assert(d.status == Status.Open)
    }
  }

  test("delays pings until reply") { tc =>
    val ctx = new Ctx(Duration.Top)
    import ctx._

    assert(n.get == 1)
    tc.advance(1.second)
    timer.tick()
    assert(n.get == 1)

    // Now immediately schedules the next one
    latch.flip()
    assert(n.get == 2)
    latch.flip()
    assert(n.get == 2)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(n.get == 3)
  }

  test("close the connection if it becomes unresponsive for too long") { tc =>
    val ctx = new Ctx
    import ctx._

    assert(d.status == Status.Open)
    tc.advance(1.milliseconds)
    latch.flip() // rtt = 1, maxPing = 1
    assert(n.get == 1)
    assert(sr.counters(Seq("close")) == 0)
    assert(d.status == Status.Open)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(d.status == Status.Open)
    (1 to 99) foreach { p =>
      tc.advance(10.milliseconds)
      timer.tick()
      assert(!d.onClose.isDefined)
    }
    tc.advance(10.milliseconds)
    timer.tick()
    assert(d.status == Status.Closed)
    assert(d.onClose.isDefined)
    assert(sr.counters(Seq("close")) == 1)
  }

  test("close if ping throws exceptions") { tc =>
    def nanoTime() = Time.now.inNanoseconds
    val timer = new MockTimer
    val sr = new InMemoryStatsReceiver
    val n = new AtomicInteger(0)
    val failAfter = 5

    def ping() = {
      if (n.incrementAndGet() >= failAfter) Future.exception(new Exception("test"))
      else Future.Done
    }

    val d = new ThresholdFailureDetector(
      ping _,
      minPeriod = 10.milliseconds,
      closeTimeout = Duration.Top,
      nanoTime = nanoTime _,
      timer = timer,
      statsReceiver = sr
    )

    for (i <- 1 until failAfter) {
      assert(n.get == i)
      assert(d.status == Status.Open)
      assert(!d.onClose.isDefined)
      tc.advance(10.milliseconds)
      timer.tick()
    }

    assert(n.get == failAfter)
    assert(d.onClose.isDefined)
    assert(sr.counters(Seq("failures")) == 1)
    assert(sr.counters(Seq("close")) == 1)
  }
}
