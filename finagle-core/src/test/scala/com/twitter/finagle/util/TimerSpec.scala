package com.twitter.finagle.util

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{CountDownLatch, Time}
import com.twitter.conversions.time._

object TimerSpec extends Specification with Mockito {
  "Timer" should {
    val timer = Timer.default
    timer.acquire()
    doAfter { timer.stop() }

    val start = Time.now
    var end = Time.now
    val latch = new CountDownLatch(1)
    val task = timer.schedule(1.second.fromNow) {
      latch.countDown()
      end = Time.now
    }

    "not execute the task until it has timed out" in {
      latch.await(2.seconds) must beTrue
      (end - start).moreOrLessEquals(1.second, 10.milliseconds)
    }

    "not execute the task if it has been cancelled" in {
      task.cancel()
      latch.await(2.seconds) must beFalse
    }
  }
}
