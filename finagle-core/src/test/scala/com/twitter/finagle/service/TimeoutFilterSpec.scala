package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.conversions.time._

import com.twitter.finagle.util.Timer
import com.twitter.finagle.util.Conversions._

import com.twitter.conversions.time._
import com.twitter.util.{Try, Promise}

object TimeoutFilterSpec extends Specification with Mockito {
  "TimeoutFilter" should {
    val timer = Timer.default
    val alternative = Try(2)
    val promise = new Promise[Int].timeout(timer, 1.second, alternative)

    "cancels the request when the service succeeds" in {
      promise.setValue(1)
      promise(2.seconds) mustBe 1
    }

    "times out a request that is not successful" in {
      promise(2.seconds) mustBe alternative.get
    }
  }
}
