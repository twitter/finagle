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

  private class Ctx(closeThreshold: Int = 1000) {
    val n = new AtomicInteger(0)
    val latch = new Latch

    def ping() = {
      n.incrementAndGet()
      latch.get
    }

    val c = new AtomicInteger(0)

    def close() = {
      c.incrementAndGet()
      Future.Done
    }

    def nanoTime() = Time.now.inNanoseconds

    val timer = new MockTimer
    val d = new ThresholdFailureDetector(
      ping,
      close,
      minPeriod = 10.milliseconds,
      threshold = 2,
      windowSize = 5,
      closeThreshold = closeThreshold,
      nanoTime = nanoTime,
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
    val ctx = new Ctx(-1)
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
    assert(c.get == 0)
    assert(d.status == Status.Open)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(d.status == Status.Open)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(d.status == Status.Busy)
    (1 to 1000) foreach { _ =>
      tc.advance(10.milliseconds)
      timer.tick()
      assert(d.status == Status.Busy)
    }

    assert(c.get == 1)
  }

  testt("busy if ping throws exceptions") { tc =>
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
      () => Future.Done,
      minPeriod = 10.milliseconds,
      threshold = 2,
      windowSize = 5,
      closeThreshold = -1,
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
    assert(d.status == Status.Busy)
    assert(sr.counters(Seq("failures")) == 1)
    assert(sr.counters(Seq("marked_busy")) == 1)
  }

  testt("darkmode does not change status") { tc =>
    def nanoTime() = Time.now.inNanoseconds
    val timer = new MockTimer
    val sr = new InMemoryStatsReceiver
    val n = new AtomicInteger(0)

    def ping() = {
      if (n.getAndIncrement() > 0) Future.exception(new Exception("test"))
      else Future.Done
    }

    val d = new ThresholdFailureDetector(
      () => ping(),
      () => Future.Done,
      minPeriod = 10.milliseconds,
      threshold = 2,
      windowSize = 5,
      closeThreshold = -1,
      nanoTime = nanoTime,
      darkMode = true,
      timer = timer,
      statsReceiver = sr
    )

    // one successful ping, then fail
    tc.advance(10.milliseconds)
    timer.tick()

    assert(d.status == Status.Open)
    assert(sr.counters(Seq("marked_busy")) == 1)
  }
}