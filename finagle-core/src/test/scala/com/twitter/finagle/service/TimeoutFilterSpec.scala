package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.finagle.util.Timer
import com.twitter.conversions.time._
import com.twitter.util.Promise
import com.twitter.finagle.TimedoutRequestException
import com.twitter.finagle.Service

object TimeoutFilterSpec extends Specification with Mockito {
  "TimeoutFilter" should {
    val timer = Timer.default
    timer.acquire()
    doAfter { timer.stop() }

    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }
    val timeoutFilter = new TimeoutFilter[String, String](1.second)
    val timeoutService = timeoutFilter.andThen(service)

    "cancels the request when the service succeeds" in {
      promise.setValue("1")
      timeoutService("blah")(2.seconds) mustBe "1"
    }

    "times out a request that is not successful" in {
      timeoutService("blah")(2.seconds) must throwA[TimedoutRequestException]
    }
  }
}
