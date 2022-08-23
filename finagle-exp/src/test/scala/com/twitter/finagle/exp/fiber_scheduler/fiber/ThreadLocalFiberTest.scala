package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec

class ThreadLocalFiberTest extends FiberSchedulerSpec {

  "hasExecutor" in {
    assert((new ThreadLocalFiber).hasExecutor == false)
  }

  "wakeUp" - {
    "runs fiber if not running" in {
      var executed = List.empty[Int]
      val f = new ThreadLocalFiber
      f.add(() => executed :+= 1, true)
      f.add(() => executed :+= 2, true)
      assert(executed == List(1, 2))
    }
    "does nothing if already running" in {
      var executed = List.empty[Int]
      val f = new ThreadLocalFiber
      f.add(
        () => {
          executed :+= 1
          assert(executed == List(1))
        },
        true)
      f.add(() => executed :+= 2, true)
      assert(executed == List(1, 2))
    }
  }
}
