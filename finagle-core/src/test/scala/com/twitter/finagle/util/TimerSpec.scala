package com.twitter.finagle.util

import java.util.Collections
import java.util.concurrent.TimeUnit
import org.jboss.netty.{util => nu}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class TimerSpec extends SpecificationWithJUnit with Mockito {
  "ManagedNettyTimer" should {
    val timer = mock[nu.Timer]
    timer.stop() returns Collections.emptySet()
    val managed = new ManagedNettyTimer(() =>timer)

    "Stop the underlying timer when the reference count reaches 0" in {
      val t0, t1 = managed.make()

      t1.dispose()
      there was no(timer).stop()
      t0.dispose()
      there was one(timer).stop()
    }

    "Cancel pending timeouts when a timer is disposed" in {
      val t0, t1 = managed.make()

      val task = mock[nu.TimerTask]
      val timeout = mock[nu.Timeout]
      timer.newTimeout(any, any, any) returns timeout

      t1.get.newTimeout(task, 1, TimeUnit.MILLISECONDS)
      there was one(timer).newTimeout(any, any, any)
      there was no(timeout).cancel()

      t1.dispose()
      there was one(timeout).cancel()
      there was no(timer).stop()
    }

    "Complain when dispose() is called twice" in {
      val t = managed.make()
      t.dispose()
      t.dispose() must throwA(new IllegalArgumentException("requirement failed: stop called twice"))
    }
  }
}
