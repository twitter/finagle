package com.twitter.finagle.util

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.conversions.time._

import com.twitter.finagle.util.Conversions._
import com.twitter.util.{Try, Promise}

object RichFutureSpec extends Specification with Mockito {
  "RichFuture" should {
    "timeout" in {
      val timer = Timer.default
      timer.acquire()
      doAfter { timer.stop() }
      val alternative = Try(2)
      val richFuture = new Promise[Int].timeout(timer, 1.second) {
        alternative
      }

      "on success: propagate & cancel the timer" in {
        richFuture.setValue(1)
        richFuture(2.seconds) mustNot throwA[Exception]
      }

      "on failure: propagate" in {
        richFuture(2.seconds) mustBe alternative.get
      }
    }
  }
}
