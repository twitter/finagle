package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec

class NextPowerOfTwoTest extends FiberSchedulerSpec {

  "test" in {
    assert(NextPowerOfTwo(2) == 2)
    assert(NextPowerOfTwo(3) == 4)
    assert(NextPowerOfTwo(30) == 32)
    assert(NextPowerOfTwo(100) == 128)
  }
}
