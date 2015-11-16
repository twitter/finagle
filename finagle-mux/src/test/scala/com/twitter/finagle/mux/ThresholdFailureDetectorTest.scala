package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class ThresholdFailureDetectorTest extends FunSuite
  with AssertionsForJUnit
  with Eventually
  with IntegrationPatience {
  def testt(desc: String)(f: TimeControl => Unit): Unit =
    test(desc) {
      Time.withCurrentTimeFrozen(f)
    }

  private class Ctx(closeTimeout: Duration = 1000.milliseconds) {
    val n = new AtomicInteger(0)
    val latch = new Latch

    def ping() = {
      n.incrementAndGet()
      latch.get
    }

    def nanoTime() = Time.now.inNanoseconds
    val sr = new InMemoryStatsReceiver

    val timer = new MockTimer
    val d = new ThresholdFailureDetector(
      ping,
      minPeriod = 10.milliseconds,
      threshold = 2,
      windowSize = 5,
      closeTimeout = closeTimeout,
      nanoTime = nanoTime,
      statsReceiver = sr,
      timer = timer
    )
  }

  testt("pings every minPeriod") { tc =>
    val ctx = new Ctx
    import ctx._

    assert(d.status == Status.Busy)

    for (i <- Seq.range(1, 10)) {
      assert(n.get == i)
      tc.advance(10.milliseconds)
      timer.tick()
      latch.flip()
      assert(d.status == Status.Open)
    }
  }

  testt("delays pings until reply") { tc =>
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

  testt("mark suspect when reaching threshold; recover") { tc =>
    val ctx = new Ctx
    import ctx._

    assert(n.get == 1)
    tc.advance(2.milliseconds)
    latch.flip() // rtt = 2, maxPing = 2
    // We got the reply, but we're not scheduling another ping yet.
    assert(n.get == 1)

    assert(d.status == Status.Open)
    tc.advance(5.milliseconds)
    timer.tick()
    assert(d.status == Status.Open)

    tc.advance(10.milliseconds)

    // One more ping; we're not going to reply for a little while.
    assert(n.get == 1)
    timer.tick()
    assert(n.get == 2)

    tc.advance(2.milliseconds)
    assert(d.status == Status.Open)
    // this crosses 2*maxPing
    tc.advance(3.milliseconds)
    timer.tick()
    assert(d.status == Status.Busy)
    val until = Status.whenOpen(d.status)
    assert(!until.isDefined)

    timer.tick()
    assert(n.get == 2)

    tc.advance(5.milliseconds)
    latch.flip() // rtt = 10, maxPing = 10

    eventually {
      assert(until.isDone)
    }

    assert(d.status == Status.Open)

    timer.tick()
    assert(n.get == 3)

    // cutoff is 20
    assert(d.status == Status.Open)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(d.status == Status.Open)
    tc.advance(9.milliseconds)
    timer.tick()
    assert(d.status == Status.Open)
    tc.advance(2.milliseconds)
    timer.tick()
    assert(d.status == Status.Busy)

    // Recover again.
    latch.flip()
    assert(d.status == Status.Open)
  }

  testt("close the connection if it becomes unresponsive for too long") { tc =>
    val ctx = new Ctx
    import ctx._

    assert(d.status == Status.Busy)
    tc.advance(1.milliseconds)
    latch.flip() // rtt = 1, maxPing = 1
    assert(n.get == 1)
    assert(sr.counters.get(Seq("close")).isEmpty)
    assert(d.status == Status.Open)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(d.status == Status.Open)
    // after 10ms mark busy, keep in busy state for 1000ms until it closes
    (1 to 99) foreach { p =>
      tc.advance(10.milliseconds)
      timer.tick()
      assert(d.status == Status.Busy)
    }
    tc.advance(10.milliseconds)
    timer.tick()
    assert(d.status == Status.Closed)
    assert(sr.counters(Seq("close")) == 1)
  }

  testt("close if ping throws exceptions") { tc =>
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
      ping,
      minPeriod = 10.milliseconds,
      threshold = 2,
      windowSize = 5,
      closeTimeout = Duration.Top,
      nanoTime = nanoTime,
      timer = timer,
      statsReceiver = sr
    )

    for (i <- 1 until failAfter) {
      assert(n.get == i)
      assert(d.status == Status.Open)
      tc.advance(10.milliseconds)
      timer.tick()
    }

    assert(n.get == failAfter)
    assert(sr.counters(Seq("failures")) == 1)
    assert(sr.counters(Seq("close")) == 1)
    assert(sr.counters.get(Seq("marked_busy")).isEmpty)
  }
}