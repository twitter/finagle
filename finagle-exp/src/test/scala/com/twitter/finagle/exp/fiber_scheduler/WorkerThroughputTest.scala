package com.twitter.finagle.exp.fiber_scheduler

import com.twitter.util.MockTimer
import com.twitter.util.Time
import com.twitter.util.TimeControl
import com.twitter.finagle.exp.fiber_scheduler.util.NextPowerOfTwo

class WorkerThroughputTest extends FiberSchedulerSpec {

  "fixed" in new Context {
    Time.withCurrentTimeFrozen { tc =>
      tp = 100
      val t = newWorkerThroughput()
      advance(window, tc)
      assert(t.perMs() == expected(tp))
      t.stop()
    }
  }

  "variable" in new Context {
    Time.withCurrentTimeFrozen { tc =>
      tp = 100
      val t = newWorkerThroughput()
      advance(window, tc)
      assert(t.perMs() == expected(tp))

      tp = 50

      // doesn't wait for a full window to update
      advance(window / 2, tc)
      assert(t.perMs() > expected(tp))

      advance(window / 2, tc)
      assert(t.perMs() == expected(tp))
      t.stop()
    }
  }

  "keeps a mean estimate" in new Context {
    Time.withCurrentTimeFrozen { tc =>
      tp = 10
      val t = newWorkerThroughput()
      advance(window * 1000, tc)
      val mean = WorkerThroughput.meanEstimateMs()
      val diff = mean - expected(tp)
      assert(diff.abs < 0.01)
      t.stop()
    }
  }

  // currently flaky, ignoring for now
  "uses the mean estimate as the initial throughput" ignore new Context {
    Time.withCurrentTimeFrozen { tc =>
      tp = 10
      val t = newWorkerThroughput()
      advance(window * 1000, tc)
      val mean = WorkerThroughput.meanEstimateMs()
      val t2 = newWorkerThroughput()
      assert(t2.perMs() == mean)
      t.stop()
      t2.stop()
    }
  }

  trait Context {
    val timer = new MockTimer
    var tp = 10
    var last = 0
    def curr = {
      last += tp
      last
    }
    def newWorkerThroughput() = WorkerThroughput(curr)(timer)
    val window = NextPowerOfTwo(Config.Scheduling.workerThroughputWindow)
    val interval = Config.Scheduling.workerThroughputInterval
    def advance(cycles: Int, tc: TimeControl) = {
      for (_ <- 0 until cycles) {
        tc.advance(interval)
        timer.tick()
      }
    }
    def expected(increment: Int) =
      increment.toDouble / Config.Scheduling.workerThroughputInterval.inMillis
  }
}
