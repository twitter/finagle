package com.twitter.finagle.util

import java.util.concurrent.TimeUnit

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{ArgumentCaptor, Matchers}

import org.jboss.netty.util.{Timer, Timeout, TimerTask}

import com.twitter.conversions.time._
import com.twitter.util.{Promise, Throw, Return}

object FutureSpec extends Specification with Mockito {
  "Future.timeout" should {
    val timer = mock[Timer]
    val richTimer = new RichTimer(timer)
    val timeout = mock[Timeout]
    val taskCaptor = ArgumentCaptor.forClass(classOf[TimerTask])
    timer.newTimeout(
      taskCaptor.capture,
      Matchers.eq(10000L),
      Matchers.eq(TimeUnit.MILLISECONDS)) returns timeout

    val promise = new Promise[Unit]
    val f = new RichFuture(promise)
      .timeout(timer, 10.seconds, Throw(new Exception("timed out")))
    there was one(timer).newTimeout(
      any[TimerTask], Matchers.eq(10000L), Matchers.eq(TimeUnit.MILLISECONDS))
    val timerTask = taskCaptor.getValue

    "on success: propagate & cancel the timer" in {
      f.isDefined must beFalse
      promise() = Return(())
      f() must be_==(())
      there was one(timeout).cancel()
    }

    "on failure: propagate" in {
      f.isDefined must beFalse
      timerTask.run(timeout)
      f() must throwA(new Exception("timed out"))

      // Make sure that setting the promise afterwards doesn't do
      // anything crazy:
      promise() = Return(())
    }

  } 
}
