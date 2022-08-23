package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.finagle.exp.fiber_scheduler.FiberSchedulerSpec

class ExpiringValueTest extends FiberSchedulerSpec {

  "not set" in new Context {
    assert(value() == default)
  }
  "not expired" in new Context {
    value.set(notDefault)
    assert(value() == notDefault)
  }
  "expired" in new Context {
    value.set(notDefault)
    waitForExpiration()
    assert(value() == default)
  }
  "expiration callback invoked on get with the expired value" - {
    trait ExpirationContext extends Context {
      var callbackValue = -1
      var callbackCalls = 0
      override def callback = (i: Int) => {
        callbackValue = i
        callbackCalls += 1
      }
    }
    "when value is expired" in new ExpirationContext {
      value.set(notDefault)
      waitForExpiration()
      assert(value() == default)
      assert(callbackValue == notDefault)
    }
    "only once when value is expired" in new ExpirationContext {
      value.set(notDefault)
      waitForExpiration()
      assert(value() == default)
      assert(value() == default)
      assert(callbackCalls == 1)
    }
  }

  trait Context {
    val default = 99
    val notDefault = 88
    def callback = (_: Int) => {}
    val expiration = Duration.fromMilliseconds(100)
    def waitForExpiration() = Time.sleep(expiration * 2)
    val value = new ExpiringValue(default, expiration, callback)
  }
}
