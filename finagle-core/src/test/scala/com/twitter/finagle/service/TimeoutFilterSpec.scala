package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.conversions.time._
import com.twitter.util.{Promise, Time}
import com.twitter.finagle.{IndividualRequestTimeoutException, Service, MockTimer}

class TimeoutFilterSpec extends SpecificationWithJUnit with Mockito {
  "TimeoutFilter" should {
    val timer = new MockTimer
    val promise = new Promise[String]
    val service = new Service[String, String] {
      def apply(request: String) = promise
    }
    val timeout = 1.second
    val exception = new IndividualRequestTimeoutException(timeout)
    val timeoutFilter = new TimeoutFilter[String, String](timeout, exception, timer)
    val timeoutService = timeoutFilter.andThen(service)

    "the request succeeds when the service succeeds" in {
      promise.setValue("1")
      val res = timeoutService("blah")
      res.isDefined must beTrue
      res() mustBe "1"
    }

    "times out a request that is not successful, cancels underlying" in Time.withCurrentTimeFrozen { tc =>
      val res = timeoutService("blah")
      res.isDefined must beFalse
      promise.isCancelled must beFalse
      tc.advance(2.seconds)
      timer.tick()
      res.isDefined must beTrue
      promise.isCancelled must beTrue
     res() must throwA(exception)
    }
  }
}
