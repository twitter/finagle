package com.twitter.finagle.exp.fiber_scheduler

import java.util.Random
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.finagle.exp.fiber_scheduler.util.Optimizer

class OptimizerTest extends FiberSchedulerSpec {

  import Optimizer._

  "fixed target" in new Context {
    withOptimizer {
      verify(target)
    }
  }

  "moving target" in new Context {
    withOptimizer {
      target = 10
      verify(target)
      target += 5
      verify(target)
      target -= 5
      verify(target)
    }
  }

  "cliff" in new Context {
    withOptimizer {
      target = 20
      cliff = 10
      verify(cliff - 1)
    }
  }

  "valley" in new Context {
    withOptimizer {
      curr = 20
      target = 5
      valley = 10
      verify(valley + 1)
    }
  }

  "cliff == valley + 1" in new Context {
    withOptimizer {
      target = 20
      cliff = 10
      valley = 9
      verify(cliff - 1)
    }
  }

  private trait Context {
    def cycle = Duration.fromMilliseconds(1)
    @volatile var curr = 5
    @volatile var target = 15
    @volatile var cliff = 20
    @volatile var valley = 2
    val rand = new Random(1)
    def score = Score(() => {
      (1000D - 50 * Math.abs(target - curr) + rand.nextInt(10)).intValue()
    })
    var sleepCount = 0
    def sleep() = {
      assert(cycle * sleepCount < Duration.fromSeconds(5), "timeout")
      Time.sleep(cycle)
      sleepCount += 1
    }
    def waitFor(cond: => Boolean) =
      while (!cond) {
        sleep()
      }
    def verify(v: Int) = {
      waitFor(curr == v)
      for (_ <- 0 until 500) {
        assert(Math.abs(curr - v) <= 1)
        sleep()
      }
    }
    def cliffs = List.empty[Limit]
    def valleys = List.empty[Limit]
    def min = 1
    def max = 30
    def withOptimizer(f: => Unit) = {
      val optimizer =
        Optimizer(
          score,
          cliffLimit = Limit(() => curr == cliff),
          valleyLimit = Limit(() => curr == valley),
          cliffExpiration = Duration.Top,
          valleyExpiration = Duration.Top,
          min = min,
          max = max,
          get = () => curr,
          up = () => Future.value(curr += 1),
          down = () => Future.value(curr -= 1),
          isIdle = () => false,
          adaptPeriod = 8,
          wavePeriod = 2,
          cycleInterval = cycle
        )
      try f
      finally optimizer.stop()
    }
  }
}
