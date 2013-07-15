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
  "TimerFromNettyTimer" should {
    val timer = mock[nu.Timer]
    val nstop = new AtomicInteger(0)
    @volatile var running = true
    timer.stop() answers { args =>
      running = false
      nstop.incrementAndGet()
      Collections.emptySet()
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
