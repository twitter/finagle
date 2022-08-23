package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.finagle.exp.fiber_scheduler.Config
import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec

class LowResClockTest extends FiberSchedulerSpec {

  "returns the current time under the configured resolution" in {
    val values = List.empty[(Time, Time)]
    val resolution = Config.Scheduling.lowResClockResolution.inNanoseconds
    val maxSkew = resolution * 4
    val deadline = LowResClock.nowNanos() + resolution * 100
    LowResClock.nowNanos()
    Time.sleep(Duration.fromMilliseconds(1000)) // wait for initialization
    while (System.nanoTime() < deadline) {
      assert(System.nanoTime() - LowResClock.nowNanos() <= maxSkew)
    }
  }
}
