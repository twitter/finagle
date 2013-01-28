package com.twitter.finagle.util

import com.twitter.conversions.time._
import com.twitter.util.TimerTask
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.{util => nu}
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class TimerSpec extends SpecificationWithJUnit with Mockito {
  "ManagedNettyTimer" should {
    val timer = mock[nu.Timer]
    val nstop = new AtomicInteger(0)
    @volatile var running = true
    timer.stop() answers { args =>
      running = false
      nstop.incrementAndGet()
      Collections.emptySet()
    }
    val shared = new SharedTimer(() => timer)

    "Stop the underlying timer when the reference count reaches 0" in {
      val t0, t1 = shared.acquire()

      t1.dispose()
      nstop.get must be_==(0)
      t0.dispose()
      nstop.get must eventually(be_==(1))
    }

    "Cancel pending timeouts when a timer is disposed" in {
      val t0, t1 = shared.acquire()

      val task = mock[nu.TimerTask]
      val timeout = mock[nu.Timeout]
      timer.newTimeout(any, any, any) returns timeout

      t1.netty.newTimeout(task, 1, TimeUnit.MILLISECONDS)
      there was one(timer).newTimeout(any, any, any)
      there was no(timeout).cancel()

      t1.dispose()
      there was one(timeout).cancel()
      there was no(timer).stop()
    }

    "Propagate cancellation and remove from pending list when timeout is cancelled" in {
      val t = shared.acquire()

      val task = mock[nu.TimerTask]
      val timeout = mock[nu.Timeout]
      timer.newTimeout(any, any, any) returns timeout

      val newTimeout = t.netty.newTimeout(task, 1, TimeUnit.MILLISECONDS)
      there was one(timer).newTimeout(any, any, any)

      there was no(timeout).cancel()
      newTimeout.cancel()
      there was one(timeout).cancel()

      t.dispose()
      there was one(timeout).cancel()  // pending is empty
      running must eventually(beFalse)
    }

    "Complain when dispose() is called twice" in {
      val t = shared.acquire()
      t.dispose()
      t.dispose() must throwA(new IllegalArgumentException("requirement failed: stop called twice"))
    }

    "Support cancelling recurring tasks" in {
      val t = new TimerFromNettyTimer(timer)

      val taskCaptor = ArgumentCaptor.forClass(classOf[nu.TimerTask])
      val firstTimeout = mock[nu.Timeout]
      firstTimeout.isCancelled returns false
      timer.newTimeout(taskCaptor.capture(), any, any) returns firstTimeout

      var task: TimerTask = null
      task = t.schedule(1.second) { task.cancel() }

      taskCaptor.getValue.run(firstTimeout)

      there was atMostOne(timer).newTimeout(any, any, any)
    }
  }
}
