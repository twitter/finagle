package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.{IndividualRequestTimeoutException, MockTimer, Service}
import com.twitter.util.{Await, Promise, Time}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class TimeoutFilterSpec extends SpecificationWithJUnit with Mockito {
  "TimeoutFilter" should {
    val timer = new MockTimer
    val promise = new Promise[String] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc => interrupted = Some(exc) }
    }
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
      Await.result(res) mustBe "1"
    }

    "times out a request that is not successful, cancels underlying" in Time.withCurrentTimeFrozen { tc =>
      val res = timeoutService("blah")
      res.isDefined must beFalse
      promise.interrupted must beNone
      tc.advance(2.seconds)
      timer.tick()
      res.isDefined must beTrue
      promise.interrupted must beLike {
        case Some(_: java.util.concurrent.TimeoutException) => true
      }
     Await.result(res) must throwA(exception)
    }
  }
}
