package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.util.{Future, Time, MockTimer, TimeControl}
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

@RunWith(classOf[JUnitRunner])
class ThresholdFailureDetectorTest extends FunSuite 
  with AssertionsForJUnit
  with Eventually 
  with IntegrationPatience {
  def testt(desc: String)(f: TimeControl => Unit): Unit =
    test(desc) { Time.withCurrentTimeFrozen(f) }

  private class Ctx {
    val n = new AtomicInteger(0)
    val latch = new Latch
    def ping() = {
      n.incrementAndGet()
      latch.get
    }
    def nanoTime() = Time.now.inNanoseconds
    val timer = new MockTimer
    val d = new ThresholdFailureDetector(
      ping, minPeriod=10.milliseconds, threshold=2, windowSize=5,
      nanoTime=nanoTime, timer=timer)
  }

  testt("pings every minPeriod") { tc =>
    val ctx = new Ctx
    import ctx._
    
    assert(d.status == Status.Busy)

    for (i <- Seq.range(1, 10)) {
      assert(n.get === i)
      tc.advance(10.milliseconds)
      timer.tick()
      latch.flip()
      assert(d.status === Status.Open)
    }
  }
  
  testt("delays pings until reply") { tc =>
    val ctx = new Ctx 
    import ctx._
    
    assert(n.get === 1)
    tc.advance(1.second)
    timer.tick()
    assert(n.get === 1)
    
    // Now immediately schedules the next one
    latch.flip()
    assert(n.get === 2)
    latch.flip()
    assert(n.get === 2)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(n.get === 3)
  }
  
  testt("mark suspect when reaching threshold; recover") { tc =>
    val ctx = new Ctx
    import ctx._
    
    assert(n.get === 1)
    tc.advance(2.milliseconds)
    latch.flip()
    // We got the reply, but we're not scheduling another ping yet.
    assert(n.get === 1)

    assert(d.status === Status.Open)
    tc.advance(5.milliseconds)
    timer.tick()
    assert(d.status === Status.Open)
    
    tc.advance(10.milliseconds)

    // One more ping; we're not going to reply for a little while.
    assert(n.get === 1)
    timer.tick()
    assert(n.get === 2)
    
    tc.advance(2.milliseconds)
    assert(d.status === Status.Open)
    // This crosses the 2x threshold
    tc.advance(3.milliseconds)
    assert(d.status == Status.Busy)
    val until = Status.whenOpen(d.status)
    assert(!until.isDefined)

    timer.tick()
    assert(n.get === 2)
    
    tc.advance(5.milliseconds)    
    latch.flip()
    
    eventually { 
      assert(until.isDone)
    }

    assert(d.status === Status.Open)
    
    timer.tick()
    assert(n.get === 3)
    
    // With this ping, we've increased our EMA (a little)
    // to about 3.45ms, so the threshold is
    // about 6.9ms.
    assert(d.status === Status.Open)
    tc.advance(5.milliseconds)
    assert(d.status === Status.Open)
    tc.advance(1.milliseconds)
    assert(d.status === Status.Open)
    tc.advance(900.microseconds)
    assert(d.status === Status.Open)
    tc.advance(100.microseconds)
    assert(d.status == Status.Busy)
    
    // Recover again.
    latch.flip()
    assert(d.status === Status.Open)
  }
}
